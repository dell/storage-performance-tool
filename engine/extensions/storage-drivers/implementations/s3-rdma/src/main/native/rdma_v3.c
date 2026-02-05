/**
 * SPT S3-RDMA V3 Native Implementation
 *
 * Direct libibverbs implementation for RDMA token generation.
 * Replaces cuObject dependency with ~200 lines of C.
 *
 * Target hardware: NVIDIA/Mellanox ConnectX-4+ with mlx5 driver
 * Dependencies: libibverbs, libmlx5 (from rdma-core)
 */

#include <jni.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/stat.h>

#include <infiniband/verbs.h>
#include <infiniband/mlx5dv.h>

/* ============================================================================
 * Constants
 * ============================================================================ */

#define DC_KEY 0xffeeddcc   /* DC key - must match server configuration */
#define MAX_TOKEN_LEN 128   /* Maximum RDMA token string length */

/* ============================================================================
 * RDMA Context Structure
 * ============================================================================ */

struct rdma_context {
    struct ibv_context *ctx;        /* Device context */
    struct ibv_pd *pd;              /* Protection Domain */
    struct ibv_cq *cq;              /* Completion Queue */
    struct ibv_srq *srq;            /* Shared Receive Queue (for DC) */
    struct ibv_qp *dct_qp;          /* DC Target QP */
    uint8_t port_num;               /* Port number (typically 1) */
    uint16_t lid;                   /* Local ID */
    uint32_t dctn;                  /* DC Target Number */
    union ibv_gid gid;              /* Global ID */
};

/* ============================================================================
 * Error Handling - RdmaException
 * ============================================================================ */

static jclass rdmaExceptionClass = NULL;
static jmethodID rdmaExceptionCtor = NULL;

/**
 * Cache the RdmaException class and constructor for efficient throwing.
 * Called once during JNI_OnLoad.
 */
static int cache_exception_class(JNIEnv *env) {
    jclass localClass = (*env)->FindClass(env,
        "com/dell/spt/storage/driver/coop/netty/http/s3/rdma/RdmaException");
    if (localClass == NULL) {
        /* RdmaException class not found - fall back to RuntimeException */
        return -1;
    }

    rdmaExceptionClass = (*env)->NewGlobalRef(env, localClass);
    (*env)->DeleteLocalRef(env, localClass);

    rdmaExceptionCtor = (*env)->GetMethodID(env, rdmaExceptionClass, "<init>",
        "(Ljava/lang/String;ILjava/lang/String;)V");

    return (rdmaExceptionCtor != NULL) ? 0 : -1;
}

/**
 * Throw RdmaException with errno details.
 */
static void throw_rdma_exception(JNIEnv *env, const char *message) {
    int err = errno;
    const char *errname = strerror(err);

    if (rdmaExceptionClass != NULL && rdmaExceptionCtor != NULL) {
        jstring jmsg = (*env)->NewStringUTF(env, message);
        jstring jerrname = (*env)->NewStringUTF(env, errname);

        jobject ex = (*env)->NewObject(env, rdmaExceptionClass, rdmaExceptionCtor,
                                       jmsg, err, jerrname);
        if (ex != NULL) {
            (*env)->Throw(env, (jthrowable)ex);
        }

        (*env)->DeleteLocalRef(env, jmsg);
        (*env)->DeleteLocalRef(env, jerrname);
    } else {
        /* Fallback to RuntimeException */
        char buf[512];
        snprintf(buf, sizeof(buf), "%s (errno=%d: %s)", message, err, errname);
        jclass rtExClass = (*env)->FindClass(env, "java/lang/RuntimeException");
        if (rtExClass != NULL) {
            (*env)->ThrowNew(env, rtExClass, buf);
        }
    }
}

/* ============================================================================
 * Device Initialization
 * ============================================================================ */

/**
 * Find and open an RDMA device.
 * If deviceName is NULL or "auto", uses the first mlx5 device found.
 * Prefers non-bonded devices (mlx5_0, mlx5_1) over bonded (mlx5_bond_X)
 * because bonded interfaces may not support DC transport.
 */
static struct ibv_context *open_device(const char *deviceName) {
    struct ibv_device **dev_list;
    struct ibv_context *ctx = NULL;
    int num_devices;
    int i;
    int preferred_idx = -1;
    int fallback_idx = -1;

    dev_list = ibv_get_device_list(&num_devices);
    if (dev_list == NULL || num_devices == 0) {
        errno = ENODEV;
        return NULL;
    }

    for (i = 0; i < num_devices; i++) {
        const char *name = ibv_get_device_name(dev_list[i]);

        /* If specific device requested, match by name */
        if (deviceName != NULL && strlen(deviceName) > 0 &&
            strcmp(deviceName, "auto") != 0) {
            if (strcmp(name, deviceName) == 0) {
                ctx = ibv_open_device(dev_list[i]);
                break;
            }
        } else {
            /* Auto-select: prefer non-bonded mlx5 devices for DC support */
            if (strncmp(name, "mlx5_", 5) == 0) {
                /* Check if this is a bonded interface */
                if (strstr(name, "bond") != NULL) {
                    /* Bonded - use as fallback only */
                    if (fallback_idx < 0) {
                        fallback_idx = i;
                    }
                } else {
                    /* Non-bonded (e.g., mlx5_0) - preferred */
                    if (preferred_idx < 0) {
                        preferred_idx = i;
                    }
                }
            }
        }
    }

    /* For auto-select, use preferred non-bonded device if available */
    if (ctx == NULL && (deviceName == NULL || strcmp(deviceName, "auto") == 0)) {
        if (preferred_idx >= 0) {
            ctx = ibv_open_device(dev_list[preferred_idx]);
            fprintf(stderr, "RDMA V3: Selected non-bonded device: %s\n",
                    ibv_get_device_name(dev_list[preferred_idx]));
        } else if (fallback_idx >= 0) {
            ctx = ibv_open_device(dev_list[fallback_idx]);
            fprintf(stderr, "RDMA V3: Warning - using bonded device: %s (DC may not work)\n",
                    ibv_get_device_name(dev_list[fallback_idx]));
        } else if (num_devices > 0) {
            /* Last resort - try first device */
            ctx = ibv_open_device(dev_list[0]);
        }
    }

    ibv_free_device_list(dev_list);
    return ctx;
}

/**
 * Query port attributes and GID.
 */
static int query_port_info(struct rdma_context *rctx) {
    struct ibv_port_attr port_attr;

    rctx->port_num = 1;  /* Use port 1 by default */

    if (ibv_query_port(rctx->ctx, rctx->port_num, &port_attr) != 0) {
        return -1;
    }
    rctx->lid = port_attr.lid;

    /* Query GID at index 0 */
    if (ibv_query_gid(rctx->ctx, rctx->port_num, 0, &rctx->gid) != 0) {
        return -1;
    }

    return 0;
}

/* ============================================================================
 * DC Target Creation (mlx5-specific)
 * ============================================================================ */

/**
 * Check if device supports DC (Dynamically Connected) transport.
 * Returns 1 if DC is supported, 0 otherwise.
 */
static int check_dc_support(struct ibv_context *ctx) {
    struct mlx5dv_context dv_ctx = {0};
    dv_ctx.comp_mask = MLX5DV_CONTEXT_MASK_DC_ODP_CAPS;

    if (mlx5dv_query_device(ctx, &dv_ctx) != 0) {
        /* Query failed - assume DC not supported */
        return 0;
    }

    /* Check for DC capability flags if available */
    /* For now, return 1 if query succeeded - the device is mlx5 */
    return 1;
}

/**
 * Create a DC Target QP using mlx5dv extensions.
 */
static int create_dc_target(struct rdma_context *rctx) {
    struct ibv_srq_init_attr srq_attr = {0};
    struct ibv_qp_init_attr_ex qp_attr = {0};
    struct mlx5dv_qp_init_attr mlx5_qp_attr = {0};

    /* Check if device supports DC transport */
    if (!check_dc_support(rctx->ctx)) {
        fprintf(stderr, "RDMA V3: Device does not support DC transport\n");
        errno = ENOTSUP;
        return -1;
    }

    /* Create Completion Queue */
    rctx->cq = ibv_create_cq(rctx->ctx, 128, NULL, NULL, 0);
    if (rctx->cq == NULL) {
        fprintf(stderr, "RDMA V3: Failed to create CQ (errno=%d: %s)\n",
                errno, strerror(errno));
        return -1;
    }

    /* Create Shared Receive Queue for DC Target */
    srq_attr.attr.max_wr = 128;
    srq_attr.attr.max_sge = 1;
    rctx->srq = ibv_create_srq(rctx->pd, &srq_attr);
    if (rctx->srq == NULL) {
        fprintf(stderr, "RDMA V3: Failed to create SRQ (errno=%d: %s)\n",
                errno, strerror(errno));
        ibv_destroy_cq(rctx->cq);
        rctx->cq = NULL;
        return -1;
    }

    /* Create DC Target QP using mlx5dv extension */
    qp_attr.qp_type = IBV_QPT_DRIVER;
    qp_attr.send_cq = rctx->cq;
    qp_attr.recv_cq = rctx->cq;
    qp_attr.srq = rctx->srq;
    qp_attr.pd = rctx->pd;
    qp_attr.comp_mask = IBV_QP_INIT_ATTR_PD;

    mlx5_qp_attr.comp_mask = MLX5DV_QP_INIT_ATTR_MASK_DC;
    mlx5_qp_attr.dc_init_attr.dc_type = MLX5DV_DCTYPE_DCT;
    mlx5_qp_attr.dc_init_attr.dct_access_key = DC_KEY;

    rctx->dct_qp = mlx5dv_create_qp(rctx->ctx, &qp_attr, &mlx5_qp_attr);
    if (rctx->dct_qp == NULL) {
        fprintf(stderr, "RDMA V3: Failed to create DC Target QP (errno=%d: %s)\n",
                errno, strerror(errno));
        ibv_destroy_srq(rctx->srq);
        ibv_destroy_cq(rctx->cq);
        rctx->srq = NULL;
        rctx->cq = NULL;
        return -1;
    }

    rctx->dctn = rctx->dct_qp->qp_num;

    /* Transition DCT to RTR state */
    struct ibv_qp_attr qp_modify_attr = {0};
    qp_modify_attr.qp_state = IBV_QPS_INIT;
    qp_modify_attr.port_num = rctx->port_num;
    qp_modify_attr.pkey_index = 0;

    if (ibv_modify_qp(rctx->dct_qp, &qp_modify_attr,
                      IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT) != 0) {
        goto cleanup;
    }

    /* For DCT (DC Target), we only need MIN_RNR_TIMER and PATH_MTU
     * DO NOT set IBV_QP_AV - that's for DC Initiator only */
    qp_modify_attr.qp_state = IBV_QPS_RTR;
    qp_modify_attr.path_mtu = IBV_MTU_4096;
    qp_modify_attr.min_rnr_timer = 12;

    if (ibv_modify_qp(rctx->dct_qp, &qp_modify_attr,
                      IBV_QP_STATE | IBV_QP_MIN_RNR_TIMER | IBV_QP_PATH_MTU) != 0) {
        goto cleanup;
    }

    return 0;

cleanup:
    ibv_destroy_qp(rctx->dct_qp);
    ibv_destroy_srq(rctx->srq);
    ibv_destroy_cq(rctx->cq);
    rctx->dct_qp = NULL;
    rctx->srq = NULL;
    rctx->cq = NULL;
    return -1;
}

/* ============================================================================
 * Token Generation
 * ============================================================================ */

/**
 * Format RDMA token string.
 * Format: addr:size:rkey:lid:dctn:g:gid
 */
static int format_token(struct rdma_context *rctx, void *addr, uint32_t size,
                        uint32_t rkey, char *out, size_t out_len) {
    char gid_str[33];

    /* Format GID as 32 hex characters */
    for (int i = 0; i < 16; i++) {
        snprintf(gid_str + i * 2, 3, "%02x", rctx->gid.raw[i]);
    }

    int written = snprintf(out, out_len,
        "%016lx:%08x:%08x:%04x:%06x:1:%s",
        (unsigned long)(uintptr_t)addr,
        size,
        rkey,
        rctx->lid,
        rctx->dctn,
        gid_str);

    return (written > 0 && (size_t)written < out_len) ? 0 : -1;
}

/* ============================================================================
 * JNI Functions
 * ============================================================================ */

/*
 * Class:     com_dell_spt_storage_driver_coop_netty_http_s3_rdma_RdmaTransport
 * Method:    nativeInit
 * Signature: (Ljava/lang/String;)J
 */
JNIEXPORT jlong JNICALL
Java_com_dell_spt_storage_driver_coop_netty_http_s3_rdma_RdmaTransport_nativeInitV3(
        JNIEnv *env, jobject self, jstring deviceName) {

    struct rdma_context *rctx = NULL;
    const char *device_str = NULL;

    /* Allocate context */
    rctx = calloc(1, sizeof(struct rdma_context));
    if (rctx == NULL) {
        throw_rdma_exception(env, "Failed to allocate RDMA context");
        return 0;
    }

    /* Get device name string */
    if (deviceName != NULL) {
        device_str = (*env)->GetStringUTFChars(env, deviceName, NULL);
    }

    /* Open device */
    rctx->ctx = open_device(device_str);
    if (device_str != NULL) {
        (*env)->ReleaseStringUTFChars(env, deviceName, device_str);
    }

    if (rctx->ctx == NULL) {
        free(rctx);
        throw_rdma_exception(env, "Failed to open RDMA device");
        return 0;
    }

    /* Allocate Protection Domain */
    rctx->pd = ibv_alloc_pd(rctx->ctx);
    if (rctx->pd == NULL) {
        ibv_close_device(rctx->ctx);
        free(rctx);
        throw_rdma_exception(env, "Failed to allocate Protection Domain");
        return 0;
    }

    /* Query port info (LID, GID) */
    if (query_port_info(rctx) != 0) {
        ibv_dealloc_pd(rctx->pd);
        ibv_close_device(rctx->ctx);
        free(rctx);
        throw_rdma_exception(env, "Failed to query port info");
        return 0;
    }

    /* Create DC Target */
    if (create_dc_target(rctx) != 0) {
        ibv_dealloc_pd(rctx->pd);
        ibv_close_device(rctx->ctx);
        free(rctx);
        throw_rdma_exception(env, "Failed to create DC Target");
        return 0;
    }

    return (jlong)(uintptr_t)rctx;
}

/*
 * Class:     com_dell_spt_storage_driver_coop_netty_http_s3_rdma_RdmaTransport
 * Method:    nativeClose
 * Signature: (J)V
 */
JNIEXPORT void JNICALL
Java_com_dell_spt_storage_driver_coop_netty_http_s3_rdma_RdmaTransport_nativeCloseV3(
        JNIEnv *env, jobject self, jlong handle) {

    struct rdma_context *rctx = (struct rdma_context *)(uintptr_t)handle;
    if (rctx == NULL) return;

    if (rctx->dct_qp) ibv_destroy_qp(rctx->dct_qp);
    if (rctx->srq) ibv_destroy_srq(rctx->srq);
    if (rctx->cq) ibv_destroy_cq(rctx->cq);
    if (rctx->pd) ibv_dealloc_pd(rctx->pd);
    if (rctx->ctx) ibv_close_device(rctx->ctx);

    free(rctx);
}

/*
 * Class:     com_dell_spt_storage_driver_coop_netty_http_s3_rdma_RdmaTransport
 * Method:    nativeRegisterBuffer
 * Signature: (JLjava/nio/ByteBuffer;I)J
 */
JNIEXPORT jlong JNICALL
Java_com_dell_spt_storage_driver_coop_netty_http_s3_rdma_RdmaTransport_nativeRegisterBuffer(
        JNIEnv *env, jobject self, jlong handle, jobject buffer, jint size) {

    struct rdma_context *rctx = (struct rdma_context *)(uintptr_t)handle;
    if (rctx == NULL) {
        throw_rdma_exception(env, "Invalid RDMA context handle");
        return 0;
    }

    void *buf_ptr = (*env)->GetDirectBufferAddress(env, buffer);
    if (buf_ptr == NULL) {
        throw_rdma_exception(env, "Buffer must be a direct ByteBuffer");
        return 0;
    }

    struct ibv_mr *mr = ibv_reg_mr(rctx->pd, buf_ptr, (size_t)size,
        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);

    if (mr == NULL) {
        throw_rdma_exception(env, "Failed to register memory region");
        return 0;
    }

    return (jlong)(uintptr_t)mr;
}

/*
 * Class:     com_dell_spt_storage_driver_coop_netty_http_s3_rdma_RdmaTransport
 * Method:    nativeDeregisterBuffer
 * Signature: (JJ)V
 */
JNIEXPORT void JNICALL
Java_com_dell_spt_storage_driver_coop_netty_http_s3_rdma_RdmaTransport_nativeDeregisterBuffer(
        JNIEnv *env, jobject self, jlong handle, jlong mrHandle) {

    struct ibv_mr *mr = (struct ibv_mr *)(uintptr_t)mrHandle;
    if (mr != NULL) {
        ibv_dereg_mr(mr);
    }
}

/*
 * Class:     com_dell_spt_storage_driver_coop_netty_http_s3_rdma_RdmaTransport
 * Method:    nativeGenerateToken
 * Signature: (JJI)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL
Java_com_dell_spt_storage_driver_coop_netty_http_s3_rdma_RdmaTransport_nativeGenerateToken(
        JNIEnv *env, jobject self, jlong handle, jlong mrHandle, jint size) {

    struct rdma_context *rctx = (struct rdma_context *)(uintptr_t)handle;
    struct ibv_mr *mr = (struct ibv_mr *)(uintptr_t)mrHandle;

    if (rctx == NULL || mr == NULL) {
        throw_rdma_exception(env, "Invalid handle for token generation");
        return NULL;
    }

    char token[MAX_TOKEN_LEN];
    if (format_token(rctx, mr->addr, (uint32_t)size, mr->rkey,
                     token, sizeof(token)) != 0) {
        throw_rdma_exception(env, "Failed to format RDMA token");
        return NULL;
    }

    return (*env)->NewStringUTF(env, token);
}

/*
 * Class:     com_dell_spt_storage_driver_coop_netty_http_s3_rdma_RdmaTransport
 * Method:    nativeIsRdmaAvailable
 * Signature: ()I
 */
JNIEXPORT jint JNICALL
Java_com_dell_spt_storage_driver_coop_netty_http_s3_rdma_RdmaTransport_nativeIsRdmaAvailableV3(
        JNIEnv *env, jobject self) {

    /* Check for RDMA device nodes */
    struct stat st;
    if (stat("/dev/infiniband", &st) != 0 || !S_ISDIR(st.st_mode)) {
        return 0;
    }

    /* Check for kernel RDMA subsystem */
    if (stat("/sys/class/infiniband", &st) != 0 || !S_ISDIR(st.st_mode)) {
        return 0;
    }

    /* Try to enumerate devices */
    int num_devices = 0;
    struct ibv_device **dev_list = ibv_get_device_list(&num_devices);
    if (dev_list == NULL || num_devices == 0) {
        if (dev_list) ibv_free_device_list(dev_list);
        return 0;
    }

    /* Look for mlx5 device specifically */
    int found_mlx5 = 0;
    for (int i = 0; i < num_devices; i++) {
        const char *name = ibv_get_device_name(dev_list[i]);
        if (strncmp(name, "mlx5", 4) == 0) {
            found_mlx5 = 1;
            break;
        }
    }

    ibv_free_device_list(dev_list);
    return found_mlx5 ? 1 : 0;
}

/* ============================================================================
 * JNI Lifecycle
 * ============================================================================ */

JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM *vm, void *reserved) {
    JNIEnv *env;
    if ((*vm)->GetEnv(vm, (void **)&env, JNI_VERSION_1_8) != JNI_OK) {
        return JNI_ERR;
    }

    /* Cache RdmaException class for efficient throwing */
    cache_exception_class(env);

    return JNI_VERSION_1_8;
}

JNIEXPORT void JNICALL JNI_OnUnload(JavaVM *vm, void *reserved) {
    JNIEnv *env;
    if ((*vm)->GetEnv(vm, (void **)&env, JNI_VERSION_1_8) != JNI_OK) {
        return;
    }

    if (rdmaExceptionClass != NULL) {
        (*env)->DeleteGlobalRef(env, rdmaExceptionClass);
        rdmaExceptionClass = NULL;
    }
}
