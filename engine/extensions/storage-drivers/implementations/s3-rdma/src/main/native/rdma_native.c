/**
 * SPT S3-RDMA Native Implementation
 *
 * Direct libibverbs/mlx5dv implementation for RDMA token generation.
 * Uses librdmacm for device/address resolution to support bonded interfaces.
 *
 * Target hardware: NVIDIA/Mellanox ConnectX-4+ with mlx5 driver
 * Dependencies: libibverbs, libmlx5, librdmacm (from rdma-core)
 */

#define _GNU_SOURCE /* CPU_ALLOC and the affinity calls */

#include <jni.h>
#include <sched.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <stdarg.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <inttypes.h>

#include <infiniband/verbs.h>
#include <infiniband/mlx5dv.h>
#include <rdma/rdma_cma.h>

/* ============================================================================
 * Constants
 * ============================================================================ */

#define DC_KEY 0xffeeddcc   /* DC access key - must match Dell ECS RDMA server configuration.
                             * This is the standard value used by Dell ECS and NVIDIA cuObject.
                             * If the server uses a different key, this constant must be updated. */
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
    enum ibv_mtu active_mtu;        /* Active port MTU */
    union ibv_gid gid;              /* Global ID */
    int gid_index;                  /* GID index (for RoCE v2) */
    int use_cm;                     /* Whether rdma_cm was used */
};

/* Forward declarations */
static void throw_rdma_exception(JNIEnv *env, const char *message);
static void log_warn(JNIEnv *env, const char *fmt, ...);

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
        rdma_destroy_id(cm_id);
        return NULL;
    }

    if (cm_id->verbs == NULL) {
        rdma_destroy_id(cm_id);
        return NULL;
    }

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
        } else if (fallback_idx >= 0) {
            ctx = ibv_open_device(dev_list[fallback_idx]);
        } else if (num_devices > 0) {
            ctx = ibv_open_device(dev_list[0]);
        }
    }

    ibv_free_device_list(dev_list);
    return ctx;
}

/* ============================================================================
 * Query Port Info - Find RoCE v2 GID for our local IP
 * ============================================================================ */

/**
 * The index of the RoCE v2 GID that holds exactly this IPv4 address, or -1 when none does.
 * IPv4 addresses are stored as IPv4-mapped IPv6 addresses in the GID table.
 */
static int scan_roce_v2_gid(struct ibv_context *ctx, uint8_t port,
                            const struct in_addr *ip, union ibv_gid *out_gid) {
    uint8_t ipv4_mapped[16] = {0,0,0,0,0,0,0,0,0,0,0xff,0xff,0,0,0,0};

    memcpy(&ipv4_mapped[12], &ip->s_addr, 4);

    for (int i = 0; i < 128; i++) {
        union ibv_gid gid;
        if (ibv_query_gid(ctx, port, i, &gid) != 0) break;

        /* Check for zero GID (end of table or unused entry) */
        int all_zero = 1;
        for (int j = 0; j < 16; j++) {
            if (gid.raw[j] != 0) { all_zero = 0; break; }
        }
        if (all_zero) continue;

        /* Check if this GID matches our IPv4-mapped address */
        if (memcmp(gid.raw, ipv4_mapped, 16) == 0) {
            /* Check GID type via sysfs - we want RoCE v2 */
            char path[256];
            char type[32];
            snprintf(path, sizeof(path),
                     "/sys/class/infiniband/%s/ports/%d/gid_attrs/types/%d",
                     ibv_get_device_name(ctx->device), port, i);
            FILE *f = fopen(path, "r");
            if (f) {
                if (fgets(type, sizeof(type), f)) {
                    type[strcspn(type, "\n")] = 0;
                    if (strstr(type, "RoCE v2") != NULL) {
                        *out_gid = gid;
                        fclose(f);
                        return i;
                    }
                }
                fclose(f);
            }
        }
    }
    return -1;
}

/**
 * Find the RoCE v2 GID index for a given local IP address.
 * For RoCE (Ethernet), we need to use the correct GID type.
 * Without a match it falls back to well-known indexes, which only the default route accepts.
 */
static int find_roce_v2_gid_index(JNIEnv *env, struct ibv_context *ctx, uint8_t port,
                                   const char *local_ip, union ibv_gid *out_gid) {
    struct in_addr ip;

    if (local_ip == NULL || strlen(local_ip) == 0) {
        /* No IP specified, try default GID index 3 (common for RoCE v2) */
        if (ibv_query_gid(ctx, port, 3, out_gid) == 0) {
            log_warn(env, "no local_ip specified, using default GID index 3");
            return 3;
        }
        log_warn(env, "no local_ip specified and GID index 3 unavailable, falling back to GID 0");
        return 0;  /* Fallback to GID 0 */
    }

    inet_aton(local_ip, &ip);

    const int matched = scan_roce_v2_gid(ctx, port, &ip, out_gid);
    if (matched >= 0) {
        return matched;
    }

    /* Fallback: try GID index 3 (common default for RoCE v2) */
    if (ibv_query_gid(ctx, port, 3, out_gid) == 0) {
        log_warn(env, "no RoCE v2 GID matched IP %s, falling back to GID index 3", local_ip);
        return 3;
    }

    /* Last resort: use GID 0 */
    if (ibv_query_gid(ctx, port, 0, out_gid) == 0) {
        log_warn(env, "no RoCE v2 GID matched IP %s, falling back to GID index 0", local_ip);
        return 0;
    }
    return -1;
}

static int query_port_info(JNIEnv *env, struct rdma_context *rctx, const char *local_ip,
                           char *errbuf, size_t errlen) {
    struct ibv_port_attr port_attr;

    rctx->port_num = 1;

    if (ibv_query_port(rctx->ctx, rctx->port_num, &port_attr) != 0) {
        snprintf(errbuf, errlen, "Failed to query port %d (errno=%d: %s)",
                 rctx->port_num, errno, strerror(errno));
        return -1;
    }
    rctx->lid = port_attr.lid;
    rctx->active_mtu = port_attr.active_mtu;

    /* For RoCE, find the correct GID index */
    rctx->gid_index = find_roce_v2_gid_index(env, rctx->ctx, rctx->port_num,
                                              local_ip, &rctx->gid);
    if (rctx->gid_index < 0) {
        snprintf(errbuf, errlen, "Failed to find valid RoCE v2 GID for IP %s",
                 (local_ip && strlen(local_ip) > 0) ? local_ip : "auto");
        return -1;
    }

    return 0;
}

/* ============================================================================
 * DC Target Creation
 * ============================================================================ */

static int create_dc_target(struct rdma_context *rctx,
                            char *errbuf, size_t errlen) {
    struct ibv_srq_init_attr srq_attr = {0};
    struct ibv_qp_init_attr_ex qp_attr = {0};
    struct mlx5dv_qp_init_attr mlx5_qp_attr = {0};
    int ret;

    /* Create Completion Queue */
    rctx->cq = ibv_create_cq(rctx->ctx, 128, NULL, NULL, 0);
    if (rctx->cq == NULL) {
        snprintf(errbuf, errlen, "Failed to create CQ (errno=%d: %s)",
                 errno, strerror(errno));
        return -1;
    }

    /* Create Shared Receive Queue for DC Target */
    srq_attr.attr.max_wr = 128;
    srq_attr.attr.max_sge = 1;
    rctx->srq = ibv_create_srq(rctx->pd, &srq_attr);
    if (rctx->srq == NULL) {
        snprintf(errbuf, errlen, "Failed to create SRQ (errno=%d: %s)",
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
    qp_attr.cap.max_recv_wr = 1;
    qp_attr.cap.max_recv_sge = 1;
    qp_attr.comp_mask = IBV_QP_INIT_ATTR_PD;

    mlx5_qp_attr.comp_mask = MLX5DV_QP_INIT_ATTR_MASK_DC;
    mlx5_qp_attr.dc_init_attr.dc_type = MLX5DV_DCTYPE_DCT;
    mlx5_qp_attr.dc_init_attr.dct_access_key = DC_KEY;

    rctx->dct_qp = mlx5dv_create_qp(rctx->ctx, &qp_attr, &mlx5_qp_attr);
    if (rctx->dct_qp == NULL) {
        snprintf(errbuf, errlen, "Failed to create DC Target QP (errno=%d: %s)",
                 errno, strerror(errno));
        ibv_destroy_srq(rctx->srq);
        ibv_destroy_cq(rctx->cq);
        rctx->srq = NULL;
        rctx->cq = NULL;
        return -1;
    }

    /* Transition DCT to INIT state */
    struct ibv_qp_attr qp_modify_attr = {0};
    qp_modify_attr.qp_state = IBV_QPS_INIT;
    qp_modify_attr.port_num = rctx->port_num;
    qp_modify_attr.pkey_index = 0;
    qp_modify_attr.qp_access_flags = IBV_ACCESS_REMOTE_WRITE |
                                      IBV_ACCESS_REMOTE_READ |
                                      IBV_ACCESS_LOCAL_WRITE;

    ret = ibv_modify_qp(rctx->dct_qp, &qp_modify_attr,
                        IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS);
    if (ret != 0) {
        snprintf(errbuf, errlen, "DCT INIT transition failed (errno=%d: %s)",
                 errno, strerror(errno));
        goto cleanup;
    }

    /*
     * Transition DCT to RTR state.
     * CRITICAL FOR RoCE: Must include IBV_QP_AV with ah_attr.is_global=1
     * and the correct GID index for RoCE v2.
     */
    memset(&qp_modify_attr, 0, sizeof(qp_modify_attr));
    qp_modify_attr.qp_state = IBV_QPS_RTR;
    qp_modify_attr.path_mtu = rctx->active_mtu;
    qp_modify_attr.min_rnr_timer = 12;

    /* Set AH attributes - required for RoCE */
    qp_modify_attr.ah_attr.port_num = rctx->port_num;
    qp_modify_attr.ah_attr.is_global = 1;  /* Required for RoCE (Ethernet) */
    qp_modify_attr.ah_attr.grh.sgid_index = rctx->gid_index;
    qp_modify_attr.ah_attr.grh.hop_limit = 64;
    qp_modify_attr.ah_attr.grh.traffic_class = 0;
    /* dgid left as zero for DCT (filled per-connection by initiator) */

    ret = ibv_modify_qp(rctx->dct_qp, &qp_modify_attr,
                        IBV_QP_STATE | IBV_QP_MIN_RNR_TIMER | IBV_QP_PATH_MTU | IBV_QP_AV);
    if (ret != 0) {
        snprintf(errbuf, errlen, "DCT RTR transition failed (errno=%d: %s)",
                 errno, strerror(errno));
        goto cleanup;
    }

    /* DCT number is valid after RTR transition */
    rctx->dctn = rctx->dct_qp->qp_num;

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
        "%016" PRIxPTR ":%08x:%08x:%04x:%06x:1:%s",
        (uintptr_t)addr,
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

/* Cached reference to RdmaTransport.nativeLogWarn(String) for JNI logging callback */
static jclass rdmaTransportClass = NULL;
static jmethodID logWarnMid = NULL;

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

/**
 * Cache RdmaTransport.nativeLogWarn(String) for native-to-Java log routing, once however many
 * contexts open. Best-effort: if caching fails, log_warn() silently does nothing.
 */
static void cache_logger_callback(JNIEnv *env) {
    if (rdmaTransportClass != NULL) {
        return;
    }
    jclass localClass = (*env)->FindClass(env,
        "com/dell/spt/storage/driver/coop/netty/http/s3/rdma/RdmaTransport");
    if (localClass == NULL) {
        (*env)->ExceptionClear(env);
        return;
    }
    rdmaTransportClass = (*env)->NewGlobalRef(env, localClass);
    (*env)->DeleteLocalRef(env, localClass);

    logWarnMid = (*env)->GetStaticMethodID(env, rdmaTransportClass,
        "nativeLogWarn", "(Ljava/lang/String;)V");
    if (logWarnMid == NULL) {
        (*env)->ExceptionClear(env);
    }
}

/**
 * Route a warning message through the Java logging framework.
 * Falls back to no-op if the JNI callback was not cached.
 */
static void log_warn(JNIEnv *env, const char *fmt, ...) {
    if (rdmaTransportClass == NULL || logWarnMid == NULL) return;

    char buf[512];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);

    jstring jmsg = (*env)->NewStringUTF(env, buf);
    if (jmsg != NULL) {
        (*env)->CallStaticVoidMethod(env, rdmaTransportClass, logWarnMid, jmsg);
        (*env)->DeleteLocalRef(env, jmsg);
    }
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
 * Context Teardown and the Strict Open
 * ============================================================================ */

static void destroy_rdma_context(struct rdma_context *rctx) {
    if (rctx == NULL) return;

    if (rctx->dct_qp) {
        /* Transition QP to RESET before destroy for portability across
         * verbs providers. mlx5 tolerates skipping this, but the verbs
         * spec recommends it and other providers may require it. */
        struct ibv_qp_attr attr = { .qp_state = IBV_QPS_RESET };
        ibv_modify_qp(rctx->dct_qp, &attr, IBV_QP_STATE);  /* best-effort */
        ibv_destroy_qp(rctx->dct_qp);
    }
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

/*
 * Fills rctx as nativeInit does, without its fallbacks. Returns 0, or -1 with errbuf set; the caller
 * destroys rctx on a failure, and create_dc_target leaves nothing half made.
 */
static int open_strict_context(struct rdma_context *rctx, const char *device, const char *local_ip,
                               char *errbuf, size_t errlen) {
    struct in_addr ip;
    struct ibv_port_attr port_attr;
    struct ibv_device **dev_list;
    int num_devices = 0;

    if (inet_aton(local_ip, &ip) == 0) {
        snprintf(errbuf, errlen, "the address is not IPv4");
        return -1;
    }

    dev_list = ibv_get_device_list(&num_devices);
    if (dev_list == NULL) {
        snprintf(errbuf, errlen, "cannot list RDMA devices (errno=%d: %s)", errno, strerror(errno));
        return -1;
    }
    for (int i = 0; i < num_devices; i++) {
        if (strcmp(ibv_get_device_name(dev_list[i]), device) == 0) {
            if (!mlx5dv_is_supported(dev_list[i])) {
                ibv_free_device_list(dev_list);
                snprintf(errbuf, errlen, "the device is not an mlx5 device");
                return -1;
            }
            rctx->ctx = ibv_open_device(dev_list[i]);
            if (rctx->ctx == NULL) {
                snprintf(errbuf, errlen, "open failed (errno=%d: %s)", errno, strerror(errno));
            }
            break;
        }
    }
    ibv_free_device_list(dev_list);
    if (rctx->ctx == NULL) {
        if (errbuf[0] == '\0') {
            snprintf(errbuf, errlen, "the device does not exist");
        }
        return -1;
    }

    rctx->pd = ibv_alloc_pd(rctx->ctx);
    if (rctx->pd == NULL) {
        snprintf(errbuf, errlen, "Protection Domain allocation failed (errno=%d: %s)", errno, strerror(errno));
        return -1;
    }

    rctx->port_num = 1;
    if (ibv_query_port(rctx->ctx, rctx->port_num, &port_attr) != 0) {
        snprintf(errbuf, errlen, "port 1 query failed (errno=%d: %s)", errno, strerror(errno));
        return -1;
    }
    if (port_attr.state != IBV_PORT_ACTIVE || port_attr.link_layer != IBV_LINK_LAYER_ETHERNET) {
        snprintf(errbuf, errlen, "port 1 is not active on Ethernet");
        return -1;
    }
    rctx->lid = port_attr.lid;
    rctx->active_mtu = port_attr.active_mtu;

    rctx->gid_index = scan_roce_v2_gid(rctx->ctx, rctx->port_num, &ip, &rctx->gid);
    if (rctx->gid_index < 0) {
        snprintf(errbuf, errlen, "port 1 has no RoCE v2 GID for the address");
        return -1;
    }

    return create_dc_target(rctx, errbuf, errlen);
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
Java_com_dell_spt_storage_driver_coop_netty_http_s3_rdma_RdmaTransport_nativeInit(
        JNIEnv *env, jobject self, jstring deviceName, jstring localIp) {

    struct rdma_context *rctx = NULL;
    const char *device_str = NULL;
    const char *local_ip_str = NULL;
    int use_cm = 0;

    rctx = calloc(1, sizeof(struct rdma_context));
    if (rctx == NULL) {
        throw_rdma_exception(env, "Failed to allocate RDMA context");
        return 0;
    }

    if (deviceName != NULL) {
        device_str = (*env)->GetStringUTFChars(env, deviceName, NULL);
    }
    if (localIp != NULL) {
        local_ip_str = (*env)->GetStringUTFChars(env, localIp, NULL);
    }

    /* Try rdma_cm first (better bonding support) */
    rctx->cm_id = open_device_via_cm(local_ip_str);

    if (rctx->cm_id != NULL) {
        rctx->ctx = rctx->cm_id->verbs;
        use_cm = 1;
    } else {
        /* Fallback to direct device open */
        rctx->ctx = open_device_direct(device_str);
    }

    if (device_str != NULL) {
        (*env)->ReleaseStringUTFChars(env, deviceName, device_str);
    }

    if (rctx->ctx == NULL) {
        if (local_ip_str != NULL) {
            (*env)->ReleaseStringUTFChars(env, localIp, local_ip_str);
        }
        free(rctx);
        throw_rdma_exception(env, "Failed to open RDMA device");
        return 0;
    }

    rctx->use_cm = use_cm;

    /* Allocate Protection Domain */
    rctx->pd = ibv_alloc_pd(rctx->ctx);
    if (rctx->pd == NULL) {
        if (local_ip_str != NULL) {
            (*env)->ReleaseStringUTFChars(env, localIp, local_ip_str);
        }
        if (use_cm) rdma_destroy_id(rctx->cm_id);
        else ibv_close_device(rctx->ctx);
        free(rctx);
        throw_rdma_exception(env, "Failed to allocate Protection Domain");
        return 0;
    }

    /* Query port info - pass local_ip for RoCE GID lookup */
    char errbuf[256] = {0};
    /* Cache JNI logger callback before query_port_info (which may log warnings) */
    cache_logger_callback(env);

    if (query_port_info(env, rctx, local_ip_str, errbuf, sizeof(errbuf)) != 0) {
        if (local_ip_str != NULL) {
            (*env)->ReleaseStringUTFChars(env, localIp, local_ip_str);
        }
        ibv_dealloc_pd(rctx->pd);
        if (use_cm) rdma_destroy_id(rctx->cm_id);
        else ibv_close_device(rctx->ctx);
        free(rctx);
        throw_rdma_exception(env, errbuf[0] ? errbuf : "Failed to query port info");
        return 0;
    }

    /* local_ip no longer needed */
    if (local_ip_str != NULL) {
        (*env)->ReleaseStringUTFChars(env, localIp, local_ip_str);
    }

    /* Create DC Target */
    errbuf[0] = '\0';
    if (create_dc_target(rctx, errbuf, sizeof(errbuf)) != 0) {
        ibv_dealloc_pd(rctx->pd);
        if (use_cm) rdma_destroy_id(rctx->cm_id);
        else ibv_close_device(rctx->ctx);
        free(rctx);
        throw_rdma_exception(env, errbuf[0] ? errbuf : "Failed to create DC Target");
        return 0;
    }

    return (jlong)(uintptr_t)rctx;
}

/*
 * Opens one NIC's context with no fallback: exactly this mlx5 device, port 1 active on Ethernet, and
 * the RoCE v2 GID that holds exactly this address; then the PD and the DC target. Any failure closes
 * what it opened and throws, naming the device and the address. The default route's nativeInit keeps
 * its fallbacks.
 */
JNIEXPORT jlong JNICALL
Java_com_dell_spt_storage_driver_coop_netty_http_s3_rdma_RdmaTransport_nativeInitStrict(
        JNIEnv *env, jobject self, jstring deviceName, jstring localIp) {

    char errbuf[256] = {0};
    char message[512] = {0};
    struct rdma_context *rctx = calloc(1, sizeof(struct rdma_context));
    const char *device = deviceName != NULL ? (*env)->GetStringUTFChars(env, deviceName, NULL) : NULL;
    const char *ip = localIp != NULL ? (*env)->GetStringUTFChars(env, localIp, NULL) : NULL;

    cache_logger_callback(env);

    if (rctx == NULL || device == NULL || ip == NULL) {
        snprintf(errbuf, sizeof(errbuf), "cannot allocate the RDMA context");
    } else {
        open_strict_context(rctx, device, ip, errbuf, sizeof(errbuf));
    }
    if (errbuf[0] != '\0') {
        snprintf(message, sizeof(message), "RDMA device %s at %s: %s",
                 device != NULL ? device : "?", ip != NULL ? ip : "?", errbuf);
    }

    if (device != NULL) {
        (*env)->ReleaseStringUTFChars(env, deviceName, device);
    }
    if (ip != NULL) {
        (*env)->ReleaseStringUTFChars(env, localIp, ip);
    }
    if (message[0] != '\0') {
        destroy_rdma_context(rctx);
        throw_rdma_exception(env, message);
        return 0;
    }
    return (jlong)(uintptr_t)rctx;
}

JNIEXPORT void JNICALL
Java_com_dell_spt_storage_driver_coop_netty_http_s3_rdma_RdmaTransport_nativeClose(
        JNIEnv *env, jobject self, jlong handle) {

    destroy_rdma_context((struct rdma_context *)(uintptr_t)handle);
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
Java_com_dell_spt_storage_driver_coop_netty_http_s3_rdma_RdmaTransport_nativeIsRdmaAvailable(
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
 * Device Query (NIC inventory)
 * ============================================================================ */

/*
 * Whether a DC target can be created on the device: the same mlx5dv call the driver makes, so a
 * device whose firmware lacks DC answers 0. Returns 1, 0, or -1 when something else failed first.
 */
static jint probe_dc_target(struct ibv_context *ctx) {
    jint supported = -1;
    struct ibv_srq_init_attr srq_attr = {0};
    struct ibv_srq *srq = NULL;
    struct ibv_cq *cq = NULL;
    struct ibv_pd *pd = ibv_alloc_pd(ctx);

    if (pd != NULL) {
        cq = ibv_create_cq(ctx, 1, NULL, NULL, 0);
    }
    if (cq != NULL) {
        srq_attr.attr.max_wr = 1;
        srq_attr.attr.max_sge = 1;
        srq = ibv_create_srq(pd, &srq_attr);
    }
    if (srq != NULL) {
        struct ibv_qp_init_attr_ex qp_attr = {0};
        struct mlx5dv_qp_init_attr mlx5_qp_attr = {0};

        qp_attr.qp_type = IBV_QPT_DRIVER;
        qp_attr.send_cq = cq;
        qp_attr.recv_cq = cq;
        qp_attr.srq = srq;
        qp_attr.pd = pd;
        qp_attr.cap.max_recv_wr = 1;
        qp_attr.cap.max_recv_sge = 1;
        qp_attr.comp_mask = IBV_QP_INIT_ATTR_PD;
        mlx5_qp_attr.comp_mask = MLX5DV_QP_INIT_ATTR_MASK_DC;
        mlx5_qp_attr.dc_init_attr.dc_type = MLX5DV_DCTYPE_DCT;
        mlx5_qp_attr.dc_init_attr.dct_access_key = DC_KEY;

        struct ibv_qp *qp = mlx5dv_create_qp(ctx, &qp_attr, &mlx5_qp_attr);
        if (qp != NULL) {
            supported = 1;
            ibv_destroy_qp(qp);
        } else if (errno == EOPNOTSUPP || errno == EPROTONOSUPPORT) {
            supported = 0;
        }
        ibv_destroy_srq(srq);
    }
    if (cq != NULL) {
        ibv_destroy_cq(cq);
    }
    if (pd != NULL) {
        ibv_dealloc_pd(pd);
    }
    return supported;
}

/*
 * Reports what sysfs does not show about one device: DC support and port 1's active MTU in
 * bytes. Opens the device by its exact name, with no fallback, and releases everything.
 * Returns {dc, activeMtu}; dc is 1, 0 or -1 (unknown), the MTU -1 when unknown.
 */
JNIEXPORT jintArray JNICALL
Java_com_dell_spt_storage_driver_coop_netty_http_s3_rdma_RdmaTransport_nativeQueryDevice(
        JNIEnv *env, jclass cls, jstring deviceName) {
    jint result[2] = {-1, -1};
    const char *name = deviceName != NULL ? (*env)->GetStringUTFChars(env, deviceName, NULL) : NULL;

    if (name != NULL) {
        int num_devices = 0;
        struct ibv_device **dev_list = ibv_get_device_list(&num_devices);
        struct ibv_device *device = NULL;

        for (int i = 0; dev_list != NULL && i < num_devices; i++) {
            if (strcmp(ibv_get_device_name(dev_list[i]), name) == 0) {
                device = dev_list[i];
                break;
            }
        }
        if (device != NULL) {
            int mlx5 = mlx5dv_is_supported(device);
            struct ibv_context *ctx = ibv_open_device(device);

            if (!mlx5) {
                result[0] = 0;
            }
            if (ctx != NULL) {
                struct ibv_port_attr port_attr;
                if (ibv_query_port(ctx, 1, &port_attr) == 0
                        && port_attr.active_mtu >= IBV_MTU_256 && port_attr.active_mtu <= IBV_MTU_4096) {
                    result[1] = 128 << port_attr.active_mtu;
                }
                if (mlx5) {
                    result[0] = probe_dc_target(ctx);
                }
                ibv_close_device(ctx);
            }
        }
        if (dev_list != NULL) {
            ibv_free_device_list(dev_list);
        }
        (*env)->ReleaseStringUTFChars(env, deviceName, name);
    }

    jintArray array = (*env)->NewIntArray(env, 2);
    if (array != NULL) {
        (*env)->SetIntArrayRegion(env, array, 0, 2, result);
    }
    return array;
}

/* ============================================================================
 * JNI Lifecycle
 * ============================================================================ */

/*
 * Keeps the calling thread to the given CPUs, within those it may already use: 0, -1 when it may use
 * none of them, or an errno. No capability is needed, as a thread only narrows its own affinity.
 */
JNIEXPORT jint JNICALL
Java_com_dell_spt_storage_driver_coop_netty_http_s3_rdma_RdmaTransport_nativeConfineCurrentThread(
        JNIEnv *env, jclass cls, jintArray cpus) {

    const jsize count = (*env)->GetArrayLength(env, cpus);
    jint *list = (*env)->GetIntArrayElements(env, cpus, NULL);
    if (list == NULL) {
        return ENOMEM;
    }

    /* The kernel refuses a mask shorter than its own, so grow until the read fits. */
    int ncpus = 1024;
    for (jsize i = 0; i < count; i++) {
        if (list[i] >= ncpus) {
            ncpus = list[i] + 1;
        }
    }
    cpu_set_t *allowed = NULL;
    cpu_set_t *wanted = NULL;
    size_t size = 0;
    int result = 0;
    for (;;) {
        allowed = CPU_ALLOC(ncpus);
        if (allowed == NULL) {
            result = ENOMEM;
            break;
        }
        size = CPU_ALLOC_SIZE(ncpus);
        CPU_ZERO_S(size, allowed);
        if (sched_getaffinity(0, size, allowed) == 0) {
            break;
        }
        result = errno;
        CPU_FREE(allowed);
        allowed = NULL;
        if (result != EINVAL || ncpus > (1 << 20)) {
            break;
        }
        result = 0;
        ncpus *= 2;
    }

    if (result == 0) {
        wanted = CPU_ALLOC(ncpus);
        if (wanted == NULL) {
            result = ENOMEM;
        } else {
            CPU_ZERO_S(size, wanted);
            for (jsize i = 0; i < count; i++) {
                if (list[i] >= 0 && list[i] < ncpus && CPU_ISSET_S(list[i], size, allowed)) {
                    CPU_SET_S(list[i], size, wanted);
                }
            }
            if (CPU_COUNT_S(size, wanted) == 0) {
                result = -1;
            } else if (sched_setaffinity(0, size, wanted) != 0) {
                result = errno;
            }
            CPU_FREE(wanted);
        }
    }
    if (allowed != NULL) {
        CPU_FREE(allowed);
    }
    (*env)->ReleaseIntArrayElements(env, cpus, list, JNI_ABORT);
    return result;
}

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
