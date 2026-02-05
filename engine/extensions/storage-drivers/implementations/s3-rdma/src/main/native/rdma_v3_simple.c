/**
 * SPT S3-RDMA V3 Native Implementation - Simple variant
 *
 * Minimal implementation that focuses on memory registration.
 * Skips DC Target creation (which fails on bonded interfaces).
 * Uses dctn=0 in token - server may handle connection setup.
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
#include <rdma/rdma_cma.h>

#define MAX_TOKEN_LEN 128

struct rdma_context {
    struct rdma_cm_id *cm_id;
    struct ibv_context *ctx;
    struct ibv_pd *pd;
    uint8_t port_num;
    uint16_t lid;
    uint32_t dctn;  /* Will be 0 without DC Target */
    union ibv_gid gid;
};

/* Error handling */
static jclass rdmaExceptionClass = NULL;
static jmethodID rdmaExceptionCtor = NULL;

static void throw_rdma_exception(JNIEnv *env, const char *message) {
    int err = errno;
    const char *errname = strerror(err);
    char buf[512];
    snprintf(buf, sizeof(buf), "%s (errno=%d: %s)", message, err, errname);

    if (rdmaExceptionClass != NULL && rdmaExceptionCtor != NULL) {
        jstring jmsg = (*env)->NewStringUTF(env, buf);
        jstring jerrname = (*env)->NewStringUTF(env, errname);
        jobject ex = (*env)->NewObject(env, rdmaExceptionClass, rdmaExceptionCtor,
                                       jmsg, err, jerrname);
        if (ex != NULL) (*env)->Throw(env, (jthrowable)ex);
        (*env)->DeleteLocalRef(env, jmsg);
        (*env)->DeleteLocalRef(env, jerrname);
    } else {
        jclass rtExClass = (*env)->FindClass(env, "java/lang/RuntimeException");
        if (rtExClass != NULL) (*env)->ThrowNew(env, rtExClass, buf);
    }
}

/* Open device via rdma_cm with local IP binding */
static struct rdma_cm_id *open_device_cm(const char *local_ip) {
    struct rdma_cm_id *cm_id = NULL;
    struct sockaddr_in addr;

    if (rdma_create_id(NULL, &cm_id, NULL, RDMA_PS_TCP) != 0) {
        return NULL;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    if (local_ip && strlen(local_ip) > 0) {
        addr.sin_addr.s_addr = inet_addr(local_ip);
    } else {
        addr.sin_addr.s_addr = INADDR_ANY;
    }

    if (rdma_bind_addr(cm_id, (struct sockaddr *)&addr) != 0 || cm_id->verbs == NULL) {
        rdma_destroy_id(cm_id);
        return NULL;
    }

    return cm_id;
}

/* Query port LID and GID */
static int query_port(struct rdma_context *rctx) {
    struct ibv_port_attr attr;
    rctx->port_num = 1;

    if (ibv_query_port(rctx->ctx, rctx->port_num, &attr) != 0) return -1;
    rctx->lid = attr.lid;

    if (ibv_query_gid(rctx->ctx, rctx->port_num, 0, &rctx->gid) != 0) return -1;

    return 0;
}

/* Format token - uses dctn=0 when no DC Target */
static int format_token(struct rdma_context *rctx, void *addr, uint32_t size,
                        uint32_t rkey, char *out, size_t out_len) {
    char gid_str[33];
    for (int i = 0; i < 16; i++) {
        snprintf(gid_str + i * 2, 3, "%02x", rctx->gid.raw[i]);
    }

    return snprintf(out, out_len, "%016lx:%08x:%08x:%04x:%06x:1:%s",
        (unsigned long)(uintptr_t)addr, size, rkey, rctx->lid, rctx->dctn, gid_str);
}

/* JNI: Initialize */
JNIEXPORT jlong JNICALL
Java_com_dell_spt_storage_driver_coop_netty_http_s3_rdma_RdmaTransport_nativeInitV3(
        JNIEnv *env, jobject self, jstring deviceName) {

    struct rdma_context *rctx = calloc(1, sizeof(struct rdma_context));
    if (!rctx) {
        throw_rdma_exception(env, "Failed to allocate context");
        return 0;
    }

    /* Use hardcoded local IP for bonded interface - TODO: make configurable */
    const char *local_ip = "10.247.128.125";

    rctx->cm_id = open_device_cm(local_ip);
    if (!rctx->cm_id) {
        free(rctx);
        throw_rdma_exception(env, "Failed to open RDMA device via CM");
        return 0;
    }
    rctx->ctx = rctx->cm_id->verbs;

    fprintf(stderr, "RDMA V3 Simple: Bound to device %s\n",
            ibv_get_device_name(rctx->ctx->device));

    rctx->pd = ibv_alloc_pd(rctx->ctx);
    if (!rctx->pd) {
        rdma_destroy_id(rctx->cm_id);
        free(rctx);
        throw_rdma_exception(env, "Failed to allocate PD");
        return 0;
    }

    if (query_port(rctx) != 0) {
        ibv_dealloc_pd(rctx->pd);
        rdma_destroy_id(rctx->cm_id);
        free(rctx);
        throw_rdma_exception(env, "Failed to query port");
        return 0;
    }

    /* No DC Target - set dctn=0, server may handle differently */
    rctx->dctn = 0;

    fprintf(stderr, "RDMA V3 Simple: Initialized (lid=%u, no DCT)\n", rctx->lid);

    return (jlong)(uintptr_t)rctx;
}

/* JNI: Close */
JNIEXPORT void JNICALL
Java_com_dell_spt_storage_driver_coop_netty_http_s3_rdma_RdmaTransport_nativeCloseV3(
        JNIEnv *env, jobject self, jlong handle) {

    struct rdma_context *rctx = (struct rdma_context *)(uintptr_t)handle;
    if (!rctx) return;

    if (rctx->pd) ibv_dealloc_pd(rctx->pd);
    if (rctx->cm_id) rdma_destroy_id(rctx->cm_id);
    free(rctx);
}

/* JNI: Register buffer */
JNIEXPORT jlong JNICALL
Java_com_dell_spt_storage_driver_coop_netty_http_s3_rdma_RdmaTransport_nativeRegisterBuffer(
        JNIEnv *env, jobject self, jlong handle, jobject buffer, jint size) {

    struct rdma_context *rctx = (struct rdma_context *)(uintptr_t)handle;
    if (!rctx) {
        throw_rdma_exception(env, "Invalid context");
        return 0;
    }

    void *ptr = (*env)->GetDirectBufferAddress(env, buffer);
    if (!ptr) {
        throw_rdma_exception(env, "Not a direct buffer");
        return 0;
    }

    struct ibv_mr *mr = ibv_reg_mr(rctx->pd, ptr, size,
        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);

    if (!mr) {
        throw_rdma_exception(env, "Failed to register MR");
        return 0;
    }

    return (jlong)(uintptr_t)mr;
}

/* JNI: Deregister buffer */
JNIEXPORT void JNICALL
Java_com_dell_spt_storage_driver_coop_netty_http_s3_rdma_RdmaTransport_nativeDeregisterBuffer(
        JNIEnv *env, jobject self, jlong handle, jlong mrHandle) {

    struct ibv_mr *mr = (struct ibv_mr *)(uintptr_t)mrHandle;
    if (mr) ibv_dereg_mr(mr);
}

/* JNI: Generate token */
JNIEXPORT jstring JNICALL
Java_com_dell_spt_storage_driver_coop_netty_http_s3_rdma_RdmaTransport_nativeGenerateToken(
        JNIEnv *env, jobject self, jlong handle, jlong mrHandle, jint size) {

    struct rdma_context *rctx = (struct rdma_context *)(uintptr_t)handle;
    struct ibv_mr *mr = (struct ibv_mr *)(uintptr_t)mrHandle;

    if (!rctx || !mr) {
        throw_rdma_exception(env, "Invalid handle");
        return NULL;
    }

    char token[MAX_TOKEN_LEN];
    if (format_token(rctx, mr->addr, size, mr->rkey, token, sizeof(token)) <= 0) {
        throw_rdma_exception(env, "Failed to format token");
        return NULL;
    }

    return (*env)->NewStringUTF(env, token);
}

/* JNI: Check RDMA availability */
JNIEXPORT jint JNICALL
Java_com_dell_spt_storage_driver_coop_netty_http_s3_rdma_RdmaTransport_nativeIsRdmaAvailableV3(
        JNIEnv *env, jobject self) {

    struct stat st;
    if (stat("/sys/class/infiniband", &st) != 0) return 0;

    int n = 0;
    struct ibv_device **list = ibv_get_device_list(&n);
    if (!list || n == 0) {
        if (list) ibv_free_device_list(list);
        return 0;
    }

    int found = 0;
    for (int i = 0; i < n; i++) {
        if (strncmp(ibv_get_device_name(list[i]), "mlx5", 4) == 0) {
            found = 1;
            break;
        }
    }

    ibv_free_device_list(list);
    return found;
}

/* JNI lifecycle */
JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM *vm, void *reserved) {
    JNIEnv *env;
    if ((*vm)->GetEnv(vm, (void **)&env, JNI_VERSION_1_8) != JNI_OK) return JNI_ERR;

    jclass cls = (*env)->FindClass(env,
        "com/dell/spt/storage/driver/coop/netty/http/s3/rdma/RdmaException");
    if (cls) {
        rdmaExceptionClass = (*env)->NewGlobalRef(env, cls);
        rdmaExceptionCtor = (*env)->GetMethodID(env, rdmaExceptionClass, "<init>",
            "(Ljava/lang/String;ILjava/lang/String;)V");
        (*env)->DeleteLocalRef(env, cls);
    }

    return JNI_VERSION_1_8;
}

JNIEXPORT void JNICALL JNI_OnUnload(JavaVM *vm, void *reserved) {
    JNIEnv *env;
    if ((*vm)->GetEnv(vm, (void **)&env, JNI_VERSION_1_8) == JNI_OK && rdmaExceptionClass) {
        (*env)->DeleteGlobalRef(env, rdmaExceptionClass);
    }
}
