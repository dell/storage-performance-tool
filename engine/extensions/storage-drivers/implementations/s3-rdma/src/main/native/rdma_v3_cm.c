/**
 * SPT S3-RDMA V3 Native Implementation - rdma_cm variant
 *
 * Uses librdmacm for device/address resolution which may handle
 * bonded interfaces better than direct libibverbs device enumeration.
 *
 * Target hardware: NVIDIA/Mellanox ConnectX-4+ with mlx5 driver
 * Dependencies: libibverbs, libmlx5, librdmacm (from rdma-core)
 */

#include <jni.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <infiniband/verbs.h>
#include <infiniband/mlx5dv.h>
#include <rdma/rdma_cma.h>

/* ============================================================================
 * Constants
 * ============================================================================ */

#define DC_KEY 0xffeeddcc   /* DC key - must match server configuration */
#define MAX_TOKEN_LEN 128   /* Maximum RDMA token string length */

/* ============================================================================
 * RDMA Context Structure
 * ============================================================================ */

struct rdma_context {
    struct rdma_cm_id *cm_id;       /* CM identifier (holds device context) */
    struct ibv_context *ctx;        /* Device context */
    struct ibv_pd *pd;              /* Protection Domain */
    struct ibv_cq *cq;              /* Completion Queue */
    struct ibv_srq *srq;            /* Shared Receive Queue (for DC) */
    struct ibv_qp *dct_qp;          /* DC Target QP */
    uint8_t port_num;               /* Port number (typically 1) */
    uint16_t lid;                   /* Local ID */
    uint32_t dctn;                  /* DC Target Number */
    union ibv_gid gid;              /* Global ID */
    int use_cm;                     /* Whether rdma_cm was used */
};

/* Forward declarations */
static void throw_rdma_exception(JNIEnv *env, const char *message);

/* ============================================================================
 * rdma_cm-based Device Initialization
 * ============================================================================ */

/**
 * Open RDMA device using rdma_cm address resolution.
 * This may handle bonded interfaces better than direct ibv_open_device.
 */
static struct rdma_cm_id *open_device_via_cm(const char *local_addr) {
    struct rdma_cm_id *cm_id = NULL;
    struct sockaddr_in addr;
    int ret;

    /* Create CM ID for address resolution */
    ret = rdma_create_id(NULL, &cm_id, NULL, RDMA_PS_TCP);
    if (ret != 0) {
        fprintf(stderr, "RDMA CM: Failed to create CM ID (errno=%d: %s)\n",
                errno, strerror(errno));
        return NULL;
    }

    /* Bind to local address to resolve to a specific device */
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;

    if (local_addr != NULL && strlen(local_addr) > 0) {
        addr.sin_addr.s_addr = inet_addr(local_addr);
    } else {
        addr.sin_addr.s_addr = INADDR_ANY;
    }
    addr.sin_port = 0;  /* Any port */

    ret = rdma_bind_addr(cm_id, (struct sockaddr *)&addr);
    if (ret != 0) {
        fprintf(stderr, "RDMA CM: Failed to bind address (errno=%d: %s)\n",
                errno, strerror(errno));
        rdma_destroy_id(cm_id);
        return NULL;
    }

    if (cm_id->verbs == NULL) {
        fprintf(stderr, "RDMA CM: No RDMA device found for address\n");
        rdma_destroy_id(cm_id);
        return NULL;
    }

    fprintf(stderr, "RDMA CM: Bound to device %s\n",
            ibv_get_device_name(cm_id->verbs->device));

    return cm_id;
}

/* ============================================================================
 * Fallback: Direct libibverbs Device Open
 * ============================================================================ */

static struct ibv_context *open_device_direct(const char *deviceName) {
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

        if (deviceName != NULL && strlen(deviceName) > 0 &&
            strcmp(deviceName, "auto") != 0) {
            if (strcmp(name, deviceName) == 0) {
                ctx = ibv_open_device(dev_list[i]);
                break;
            }
        } else {
            if (strncmp(name, "mlx5_", 5) == 0) {
                if (strstr(name, "bond") != NULL) {
                    if (fallback_idx < 0) fallback_idx = i;
                } else {
                    if (preferred_idx < 0) preferred_idx = i;
                }
            }
        }
    }

    if (ctx == NULL && (deviceName == NULL || strcmp(deviceName, "auto") == 0)) {
        if (preferred_idx >= 0) {
            ctx = ibv_open_device(dev_list[preferred_idx]);
            fprintf(stderr, "RDMA V3: Selected non-bonded device: %s\n",
                    ibv_get_device_name(dev_list[preferred_idx]));
        } else if (fallback_idx >= 0) {
            ctx = ibv_open_device(dev_list[fallback_idx]);
            fprintf(stderr, "RDMA V3: Warning - using bonded device: %s\n",
                    ibv_get_device_name(dev_list[fallback_idx]));
        } else if (num_devices > 0) {
            ctx = ibv_open_device(dev_list[0]);
        }
    }

    ibv_free_device_list(dev_list);
    return ctx;
}

/* ============================================================================
 * Query Port Info
 * ============================================================================ */

static int query_port_info(struct rdma_context *rctx) {
    struct ibv_port_attr port_attr;

    rctx->port_num = 1;

    if (ibv_query_port(rctx->ctx, rctx->port_num, &port_attr) != 0) {
        return -1;
    }
    rctx->lid = port_attr.lid;

    if (ibv_query_gid(rctx->ctx, rctx->port_num, 0, &rctx->gid) != 0) {
        return -1;
    }

    return 0;
}

/* ============================================================================
 * DC Target Creation
 * ============================================================================ */

static int create_dc_target(struct rdma_context *rctx) {
    struct ibv_srq_init_attr srq_attr = {0};
    struct ibv_qp_init_attr_ex qp_attr = {0};
    struct mlx5dv_qp_init_attr mlx5_qp_attr = {0};

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

static int format_token(struct rdma_context *rctx, void *addr, uint32_t size,
                        uint32_t rkey, char *out, size_t out_len) {
    char gid_str[33];

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
 * Error Handling
 * ============================================================================ */

static jclass rdmaExceptionClass = NULL;
static jmethodID rdmaExceptionCtor = NULL;

static int cache_exception_class(JNIEnv *env) {
    jclass localClass = (*env)->FindClass(env,
        "com/dell/spt/storage/driver/coop/netty/http/s3/rdma/RdmaException");
    if (localClass == NULL) {
        return -1;
    }

    rdmaExceptionClass = (*env)->NewGlobalRef(env, localClass);
    (*env)->DeleteLocalRef(env, localClass);

    rdmaExceptionCtor = (*env)->GetMethodID(env, rdmaExceptionClass, "<init>",
        "(Ljava/lang/String;ILjava/lang/String;)V");

    return (rdmaExceptionCtor != NULL) ? 0 : -1;
}

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
        char buf[512];
        snprintf(buf, sizeof(buf), "%s (errno=%d: %s)", message, err, errname);
        jclass rtExClass = (*env)->FindClass(env, "java/lang/RuntimeException");
        if (rtExClass != NULL) {
            (*env)->ThrowNew(env, rtExClass, buf);
        }
    }
}

/* ============================================================================
 * JNI Functions
 * ============================================================================ */

/*
 * Initialize RDMA context.
 * First tries rdma_cm for better bonded interface support,
 * then falls back to direct libibverbs.
 */
JNIEXPORT jlong JNICALL
Java_com_dell_spt_storage_driver_coop_netty_http_s3_rdma_RdmaTransport_nativeInitV3(
        JNIEnv *env, jobject self, jstring deviceName) {

    struct rdma_context *rctx = NULL;
    const char *device_str = NULL;
    int use_cm = 0;

    rctx = calloc(1, sizeof(struct rdma_context));
    if (rctx == NULL) {
        throw_rdma_exception(env, "Failed to allocate RDMA context");
        return 0;
    }

    if (deviceName != NULL) {
        device_str = (*env)->GetStringUTFChars(env, deviceName, NULL);
    }

    /* Try rdma_cm first (better bonding support) */
    /* For bonded interfaces, we need a specific local IP */
    /* TODO: Get this from configuration - for now test with hardcoded IP */
    const char *local_ip = "10.247.128.125";

    fprintf(stderr, "RDMA V3: Trying rdma_cm with local IP: %s\n", local_ip);
    rctx->cm_id = open_device_via_cm(local_ip);

    if (rctx->cm_id != NULL) {
        rctx->ctx = rctx->cm_id->verbs;
        use_cm = 1;
        fprintf(stderr, "RDMA V3: Using rdma_cm for device access\n");
    } else {
        /* Fallback to direct device open */
        fprintf(stderr, "RDMA V3: rdma_cm failed, trying direct device open\n");
        rctx->ctx = open_device_direct(device_str);
    }

    if (device_str != NULL) {
        (*env)->ReleaseStringUTFChars(env, deviceName, device_str);
    }

    if (rctx->ctx == NULL) {
        free(rctx);
        throw_rdma_exception(env, "Failed to open RDMA device");
        return 0;
    }

    rctx->use_cm = use_cm;

    /* Allocate Protection Domain */
    rctx->pd = ibv_alloc_pd(rctx->ctx);
    if (rctx->pd == NULL) {
        if (use_cm) rdma_destroy_id(rctx->cm_id);
        else ibv_close_device(rctx->ctx);
        free(rctx);
        throw_rdma_exception(env, "Failed to allocate Protection Domain");
        return 0;
    }

    /* Query port info */
    if (query_port_info(rctx) != 0) {
        ibv_dealloc_pd(rctx->pd);
        if (use_cm) rdma_destroy_id(rctx->cm_id);
        else ibv_close_device(rctx->ctx);
        free(rctx);
        throw_rdma_exception(env, "Failed to query port info");
        return 0;
    }

    /* Create DC Target */
    if (create_dc_target(rctx) != 0) {
        ibv_dealloc_pd(rctx->pd);
        if (use_cm) rdma_destroy_id(rctx->cm_id);
        else ibv_close_device(rctx->ctx);
        free(rctx);
        throw_rdma_exception(env, "Failed to create DC Target");
        return 0;
    }

    return (jlong)(uintptr_t)rctx;
}

JNIEXPORT void JNICALL
Java_com_dell_spt_storage_driver_coop_netty_http_s3_rdma_RdmaTransport_nativeCloseV3(
        JNIEnv *env, jobject self, jlong handle) {

    struct rdma_context *rctx = (struct rdma_context *)(uintptr_t)handle;
    if (rctx == NULL) return;

    if (rctx->dct_qp) ibv_destroy_qp(rctx->dct_qp);
    if (rctx->srq) ibv_destroy_srq(rctx->srq);
    if (rctx->cq) ibv_destroy_cq(rctx->cq);
    if (rctx->pd) ibv_dealloc_pd(rctx->pd);

    if (rctx->use_cm && rctx->cm_id) {
        rdma_destroy_id(rctx->cm_id);
    } else if (rctx->ctx) {
        ibv_close_device(rctx->ctx);
    }

    free(rctx);
}

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

JNIEXPORT void JNICALL
Java_com_dell_spt_storage_driver_coop_netty_http_s3_rdma_RdmaTransport_nativeDeregisterBuffer(
        JNIEnv *env, jobject self, jlong handle, jlong mrHandle) {

    struct ibv_mr *mr = (struct ibv_mr *)(uintptr_t)mrHandle;
    if (mr != NULL) {
        ibv_dereg_mr(mr);
    }
}

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

JNIEXPORT jint JNICALL
Java_com_dell_spt_storage_driver_coop_netty_http_s3_rdma_RdmaTransport_nativeIsRdmaAvailableV3(
        JNIEnv *env, jobject self) {

    struct stat st;
    if (stat("/dev/infiniband", &st) != 0 || !S_ISDIR(st.st_mode)) {
        return 0;
    }

    if (stat("/sys/class/infiniband", &st) != 0 || !S_ISDIR(st.st_mode)) {
        return 0;
    }

    int num_devices = 0;
    struct ibv_device **dev_list = ibv_get_device_list(&num_devices);
    if (dev_list == NULL || num_devices == 0) {
        if (dev_list) ibv_free_device_list(dev_list);
        return 0;
    }

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
