/**
 * JNI bridge between SPT's RdmaTransport.java and rdma-object-client (libobjclient).
 *
 * This file implements native methods declared in RdmaTransport.java that wrap
 * the C++ object::Client API for RDMA-accelerated S3 operations.
 */

#include <jni.h>
#include <string>
#include <cstring>
#include <memory>
#include <sys/stat.h>

#include "obj_client.h"
#include "memory.h"

// JNI package prefix for RdmaTransport class
// com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaTransport
#define JNI_CLASS_PREFIX Java_com_dell_spt_storage_driver_coop_netty_http_s3_rdma_RdmaTransport_

// Helper macro to generate JNI function names
#define JNI_FUNC(name) JNI_CLASS_PREFIX##name

// Helper to convert jstring to std::string
static std::string jstringToString(JNIEnv *env, jstring jstr) {
    if (jstr == nullptr) {
        return "";
    }
    const char *chars = env->GetStringUTFChars(jstr, nullptr);
    if (chars == nullptr) {
        return "";
    }
    std::string result(chars);
    env->ReleaseStringUTFChars(jstr, chars);
    return result;
}

// Helper to throw a Java exception
static void throwJavaException(JNIEnv *env, const char *className, const char *message) {
    jclass exClass = env->FindClass(className);
    if (exClass != nullptr) {
        env->ThrowNew(exClass, message);
    }
}

extern "C" {

/**
 * Initialize the native RDMA client.
 *
 * Class:     com_dell_spt_storage_driver_coop_netty_http_s3_rdma_RdmaTransport
 * Method:    nativeInit
 * Signature: (Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;I)J
 *
 * @param endpoint   S3 endpoint URL (e.g., "http://10.247.128.105:9020")
 * @param accessKey  S3 access key
 * @param secretKey  S3 secret key
 * @param localIp    Local IP for RDMA binding (empty for auto)
 * @param logLevel   Log level (e.g., "WARN", "INFO", "DEBUG")
 * @param concurrency Concurrency level (0 for default)
 * @return Native handle (pointer to object::Client), or 0 on failure
 */
JNIEXPORT jlong JNICALL Java_com_dell_spt_storage_driver_coop_netty_http_s3_rdma_RdmaTransport_nativeInit(
        JNIEnv *env,
        jobject self,
        jstring endpoint,
        jstring accessKey,
        jstring secretKey,
        jstring localIp,
        jstring logLevel,
        jint concurrency) {

    try {
        std::string endpointStr = jstringToString(env, endpoint);
        std::string accessKeyStr = jstringToString(env, accessKey);
        std::string secretKeyStr = jstringToString(env, secretKey);
        std::string localIpStr = jstringToString(env, localIp);
        std::string logLevelStr = jstringToString(env, logLevel);

        // Parse endpoint to extract hostname and determine TLS
        std::string hostname;
        bool enableTls = false;

        // Simple parsing: remove protocol prefix
        size_t protoEnd = endpointStr.find("://");
        if (protoEnd != std::string::npos) {
            std::string proto = endpointStr.substr(0, protoEnd);
            enableTls = (proto == "https");
            hostname = endpointStr.substr(protoEnd + 3);
        } else {
            hostname = endpointStr;
        }

        // Remove port from hostname for the hostname field
        size_t portStart = hostname.find(':');
        std::string hostnameOnly = (portStart != std::string::npos)
            ? hostname.substr(0, portStart)
            : hostname;

        object::Config cfg = {
            .region = "us-east-1",  // Default region
            .endpoint = endpointStr,
            .hostname = hostnameOnly,
            .access_key = accessKeyStr,
            .secret_key = secretKeyStr,
            .local_ip = localIpStr,
            .enable_tls = enableTls,
            .verify_peer = false,  // Disable peer verification for internal clusters
            .ca_path = "",
            .log_level = logLevelStr.empty() ? "WARN" : logLevelStr,
            .concurrency = static_cast<uint32_t>(concurrency > 0 ? concurrency : 8),
        };

        auto *client = new object::Client(cfg);
        return reinterpret_cast<jlong>(client);

    } catch (const std::exception &e) {
        // Log error but don't throw - return 0 to indicate failure
        fprintf(stderr, "RDMA native init failed: %s\n", e.what());
        return 0;
    } catch (...) {
        fprintf(stderr, "RDMA native init failed: unknown error\n");
        return 0;
    }
}

/**
 * Close and free the native RDMA client.
 *
 * Class:     com_dell_spt_storage_driver_coop_netty_http_s3_rdma_RdmaTransport
 * Method:    nativeClose
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_dell_spt_storage_driver_coop_netty_http_s3_rdma_RdmaTransport_nativeClose(
        JNIEnv *env,
        jobject self,
        jlong handle) {

    if (handle != 0) {
        auto *client = reinterpret_cast<object::Client*>(handle);
        delete client;
    }
}

/**
 * PUT an object via RDMA.
 *
 * Class:     com_dell_spt_storage_driver_coop_netty_http_s3_rdma_RdmaTransport
 * Method:    nativePutObject
 * Signature: (JLjava/lang/String;Ljava/lang/String;Ljava/nio/ByteBuffer;I)I
 *
 * @param handle Native client handle
 * @param bucket S3 bucket name
 * @param key    S3 object key
 * @param buffer Direct ByteBuffer containing data to write
 * @param size   Number of bytes to write
 * @return HTTP status code (200-299 on success), or -1 on error
 */
JNIEXPORT jint JNICALL Java_com_dell_spt_storage_driver_coop_netty_http_s3_rdma_RdmaTransport_nativePutObject(
        JNIEnv *env,
        jobject self,
        jlong handle,
        jstring bucket,
        jstring key,
        jobject buffer,
        jint size) {

    if (handle == 0) {
        return -1;
    }

    try {
        auto *client = reinterpret_cast<object::Client*>(handle);

        // Get direct buffer address
        void *bufPtr = env->GetDirectBufferAddress(buffer);
        if (bufPtr == nullptr) {
            throwJavaException(env, "java/lang/IllegalArgumentException",
                              "Buffer must be a direct ByteBuffer");
            return -1;
        }

        object::Request req = {
            .bucket = jstringToString(env, bucket),
            .key = jstringToString(env, key),
            .user_buf_ptr = bufPtr,
            .user_buf_size = static_cast<size_t>(size),
            .headers = {},
            .use_rdma = true,
        };

        object::Response resp = {};
        int rc = client->putObject(req, resp);

        if (rc == 0) {
            return resp.status;
        } else {
            // Log error body if available
            if (!resp.error_body.empty()) {
                fprintf(stderr, "RDMA PUT failed: %s\n", resp.error_body.c_str());
            }
            return (resp.status != 0) ? resp.status : -1;
        }

    } catch (const std::exception &e) {
        fprintf(stderr, "RDMA PUT exception: %s\n", e.what());
        return -1;
    } catch (...) {
        fprintf(stderr, "RDMA PUT unknown exception\n");
        return -1;
    }
}

/**
 * GET an object via RDMA.
 *
 * Class:     com_dell_spt_storage_driver_coop_netty_http_s3_rdma_RdmaTransport
 * Method:    nativeGetObject
 * Signature: (JLjava/lang/String;Ljava/lang/String;Ljava/nio/ByteBuffer;I)I
 *
 * @param handle  Native client handle
 * @param bucket  S3 bucket name
 * @param key     S3 object key
 * @param buffer  Direct ByteBuffer to receive data
 * @param maxSize Maximum number of bytes to read
 * @return HTTP status code (200-299 on success), or -1 on error
 */
JNIEXPORT jint JNICALL Java_com_dell_spt_storage_driver_coop_netty_http_s3_rdma_RdmaTransport_nativeGetObject(
        JNIEnv *env,
        jobject self,
        jlong handle,
        jstring bucket,
        jstring key,
        jobject buffer,
        jint maxSize) {

    if (handle == 0) {
        return -1;
    }

    try {
        auto *client = reinterpret_cast<object::Client*>(handle);

        // Get direct buffer address
        void *bufPtr = env->GetDirectBufferAddress(buffer);
        if (bufPtr == nullptr) {
            throwJavaException(env, "java/lang/IllegalArgumentException",
                              "Buffer must be a direct ByteBuffer");
            return -1;
        }

        object::Request req = {
            .bucket = jstringToString(env, bucket),
            .key = jstringToString(env, key),
            .user_buf_ptr = bufPtr,
            .user_buf_size = static_cast<size_t>(maxSize),
            .headers = {},
            .use_rdma = true,
        };

        object::Response resp = {};
        int rc = client->getObject(req, resp);

        if (rc == 0) {
            return resp.status;
        } else {
            if (!resp.error_body.empty()) {
                fprintf(stderr, "RDMA GET failed: %s\n", resp.error_body.c_str());
            }
            return (resp.status != 0) ? resp.status : -1;
        }

    } catch (const std::exception &e) {
        fprintf(stderr, "RDMA GET exception: %s\n", e.what());
        return -1;
    } catch (...) {
        fprintf(stderr, "RDMA GET unknown exception\n");
        return -1;
    }
}

/**
 * Allocate RDMA-registered memory.
 *
 * Class:     com_dell_spt_storage_driver_coop_netty_http_s3_rdma_RdmaTransport
 * Method:    nativeAllocateBuffer
 * Signature: (I)J
 *
 * @param size Buffer size in bytes
 * @return Pointer to allocated memory, or 0 on failure
 */
JNIEXPORT jlong JNICALL Java_com_dell_spt_storage_driver_coop_netty_http_s3_rdma_RdmaTransport_nativeAllocateBuffer(
        JNIEnv *env,
        jobject self,
        jint size) {

    try {
        // Use host memory (CPU) allocation with RDMA registration
        void *ptr = allocateMemory(static_cast<size_t>(size), true);
        return reinterpret_cast<jlong>(ptr);
    } catch (...) {
        return 0;
    }
}

/**
 * Free RDMA-registered memory.
 *
 * Class:     com_dell_spt_storage_driver_coop_netty_http_s3_rdma_RdmaTransport
 * Method:    nativeFreeBuffer
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_dell_spt_storage_driver_coop_netty_http_s3_rdma_RdmaTransport_nativeFreeBuffer(
        JNIEnv *env,
        jobject self,
        jlong bufferPtr) {

    if (bufferPtr != 0) {
        void *ptr = reinterpret_cast<void*>(bufferPtr);
        freeMemory(ptr, true);
    }
}

/**
 * Wrap a native buffer pointer in a direct ByteBuffer.
 *
 * Class:     com_dell_spt_storage_driver_coop_netty_http_s3_rdma_RdmaTransport
 * Method:    nativeWrapBuffer
 * Signature: (JI)Ljava/nio/ByteBuffer;
 *
 * @param bufferPtr Native buffer pointer from nativeAllocateBuffer
 * @param size      Buffer size
 * @return Direct ByteBuffer wrapping the native memory
 */
JNIEXPORT jobject JNICALL Java_com_dell_spt_storage_driver_coop_netty_http_s3_rdma_RdmaTransport_nativeWrapBuffer(
        JNIEnv *env,
        jobject self,
        jlong bufferPtr,
        jint size) {

    if (bufferPtr == 0) {
        return nullptr;
    }

    void *ptr = reinterpret_cast<void*>(bufferPtr);
    return env->NewDirectByteBuffer(ptr, static_cast<jlong>(size));
}

/**
 * Check if RDMA is likely available on this system.
 *
 * Performs basic checks for RDMA hardware availability:
 * 1. Checks for /dev/infiniband directory (RDMA device nodes)
 * 2. Checks for /sys/class/infiniband (kernel RDMA subsystem)
 *
 * @return 1 if RDMA hardware appears available, 0 otherwise
 */
JNIEXPORT jint JNICALL Java_com_dell_spt_storage_driver_coop_netty_http_s3_rdma_RdmaTransport_nativeIsRdmaAvailable(
        JNIEnv* env,
        jobject self) {
    // Check for RDMA device nodes
    struct stat st;
    if (stat("/dev/infiniband", &st) == 0 && S_ISDIR(st.st_mode)) {
        // /dev/infiniband exists, check for at least one device
        if (stat("/sys/class/infiniband", &st) == 0 && S_ISDIR(st.st_mode)) {
            return 1;
        }
    }
    return 0;
}

} // extern "C"
