// JNI binding of the stream C ABI (integrations/grpc/cpp/include/
// dpumesh_stream.h) for io.dpumesh.grpc.NativeBridge. The library is opened
// with dlopen, so a test can name the in-process loopback instead. Runtime
// threads attach to the JVM on their first upcall and detach when they exit.
#include <dlfcn.h>
#include <errno.h>
#include <jni.h>
#include <pthread.h>

#include <atomic>
#include <cstring>
#include <string>

#include "dpumesh_stream.h"

namespace {

struct Api {
  int (*abi_version)(void);
  dms_runtime* (*runtime_open)(char*, size_t);
  void (*runtime_close)(dms_runtime*);
  size_t (*runtime_post_max)(const dms_runtime*);
  void (*connect)(dms_runtime*, const char*, dms_connect_fn, void*);
  int (*listen)(dms_runtime*, dms_accept_fn, void*);
  int (*stream_bind)(dms_stream*, const dms_stream_callbacks*, void*);
  ssize_t (*stream_write)(dms_stream*, const void*, size_t);
  void (*stream_resume)(dms_stream*);
  void (*stream_close)(dms_stream*);
  void (*stream_abort)(dms_stream*);
};

// One bound stream: the native handle and the Java sink it reports to. Freed
// after the sink's released() upcall, the stream's last callback.
struct JStream {
  dms_stream* handle;
  jobject sink;  // global reference
  std::atomic<bool> ended{false};
};

Api g_api;
JavaVM* g_vm = nullptr;
pthread_key_t g_detach_key;
jclass g_bridge;  // global reference
jmethodID g_on_accept;
jmethodID g_on_connect;
jmethodID g_allocate;
jmethodID g_received;
jmethodID g_writable;
jmethodID g_eof;
jmethodID g_error;
jmethodID g_released;

void DetachOnExit(void*) {
  if (g_vm != nullptr) g_vm->DetachCurrentThread();
}

// The JNIEnv of a runtime thread, attached as a daemon on first use.
JNIEnv* Env() {
  JNIEnv* env = nullptr;
  if (g_vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_8) == JNI_OK) return env;
  JavaVMAttachArgs args{JNI_VERSION_1_8, const_cast<char*>("dpumesh-runtime"), nullptr};
  if (g_vm->AttachCurrentThreadAsDaemon(reinterpret_cast<void**>(&env), &args) != JNI_OK) {
    return nullptr;
  }
  pthread_setspecific(g_detach_key, env);
  return env;
}

// Clears a pending Java exception; true when there was one.
bool Failed(JNIEnv* env) {
  if (!env->ExceptionCheck()) return false;
  env->ExceptionDescribe();
  env->ExceptionClear();
  return true;
}

void ThrowIo(JNIEnv* env, const std::string& message) {
  jclass io = env->FindClass("java/io/IOException");
  if (io != nullptr) env->ThrowNew(io, message.c_str());
}

// ---- stream callbacks, on the reactor thread ----

void* OnRecvAlloc(void* ctx, size_t len) {
  auto* s = static_cast<JStream*>(ctx);
  JNIEnv* env = Env();
  if (env == nullptr) return nullptr;
  jobject buffer = env->CallObjectMethod(s->sink, g_allocate, static_cast<jint>(len));
  if (Failed(env) || buffer == nullptr) return nullptr;
  void* address = env->GetDirectBufferAddress(buffer);
  env->DeleteLocalRef(buffer);
  return address;
}

int OnRecv(void* ctx, void*, size_t len) {
  auto* s = static_cast<JStream*>(ctx);
  JNIEnv* env = Env();
  if (env == nullptr) return DMS_RECV_OK;
  const jboolean hold = env->CallBooleanMethod(s->sink, g_received, static_cast<jint>(len));
  if (Failed(env)) return DMS_RECV_OK;
  return hold ? DMS_RECV_HOLD : DMS_RECV_OK;
}

void CallVoid(void* ctx, jmethodID method) {
  auto* s = static_cast<JStream*>(ctx);
  if (JNIEnv* env = Env()) {
    env->CallVoidMethod(s->sink, method);
    Failed(env);
  }
}

void OnWritable(void* ctx) { CallVoid(ctx, g_writable); }
void OnEof(void* ctx) { CallVoid(ctx, g_eof); }

void OnError(void* ctx, int err, const char* message) {
  auto* s = static_cast<JStream*>(ctx);
  if (JNIEnv* env = Env()) {
    jstring text = env->NewStringUTF(message != nullptr ? message : "");
    env->CallVoidMethod(s->sink, g_error, static_cast<jint>(err), text);
    Failed(env);
    env->DeleteLocalRef(text);
  }
}

void OnReleased(void* ctx) {
  auto* s = static_cast<JStream*>(ctx);
  if (JNIEnv* env = Env()) {
    env->CallVoidMethod(s->sink, g_released);
    Failed(env);
    env->DeleteGlobalRef(s->sink);
  }
  delete s;
}

const dms_stream_callbacks kCallbacks = {OnRecvAlloc, OnRecv, OnWritable,
                                         OnEof, OnError, OnReleased};

// ---- runtime callbacks, on the callback thread ----

void OnAccept(void*, dms_stream* stream) {
  JNIEnv* env = Env();
  if (env == nullptr) {
    g_api.stream_abort(stream);
    return;
  }
  env->CallStaticVoidMethod(g_bridge, g_on_accept, reinterpret_cast<jlong>(stream));
  if (Failed(env)) g_api.stream_abort(stream);
}

void OnConnect(void* ctx, dms_stream* stream, int err, const char* message) {
  JNIEnv* env = Env();
  if (env == nullptr) {
    if (stream != nullptr) g_api.stream_abort(stream);
    return;
  }
  jstring text = env->NewStringUTF(message != nullptr ? message : "");
  env->CallStaticVoidMethod(g_bridge, g_on_connect, reinterpret_cast<jlong>(ctx),
                            reinterpret_cast<jlong>(stream), static_cast<jint>(err), text);
  if (Failed(env) && stream != nullptr) g_api.stream_abort(stream);
  env->DeleteLocalRef(text);
}

bool Load(JNIEnv* env, jstring library) {
  std::string path = "libdpumesh_stream.so";
  if (library != nullptr) {
    const char* chars = env->GetStringUTFChars(library, nullptr);
    path = chars;
    env->ReleaseStringUTFChars(library, chars);
  }
  void* lib = dlopen(path.c_str(), RTLD_NOW | RTLD_LOCAL);
  if (lib == nullptr) {
    ThrowIo(env, std::string("cannot load the DPUMesh stream library: ") + dlerror());
    return false;
  }
#define LOAD(field, name)                                                   \
  g_api.field = reinterpret_cast<decltype(g_api.field)>(dlsym(lib, name)); \
  if (g_api.field == nullptr) {                                             \
    ThrowIo(env, std::string("missing ") + name);                           \
    return false;                                                           \
  }
  LOAD(abi_version, "dms_abi_version")
  LOAD(runtime_open, "dms_runtime_open")
  LOAD(runtime_close, "dms_runtime_close")
  LOAD(runtime_post_max, "dms_runtime_post_max")
  LOAD(connect, "dms_connect")
  LOAD(listen, "dms_listen")
  LOAD(stream_bind, "dms_stream_bind")
  LOAD(stream_write, "dms_stream_write")
  LOAD(stream_resume, "dms_stream_resume")
  LOAD(stream_close, "dms_stream_close")
  LOAD(stream_abort, "dms_stream_abort")
#undef LOAD
  if (g_api.abi_version() != DMS_ABI_VERSION) {
    ThrowIo(env, "libdpumesh_stream ABI mismatch");
    return false;
  }
  return true;
}

}  // namespace

extern "C" {

JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM* vm, void*) {
  g_vm = vm;
  pthread_key_create(&g_detach_key, DetachOnExit);
  JNIEnv* env = nullptr;
  if (vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_8) != JNI_OK) return JNI_ERR;
  jclass bridge = env->FindClass("io/dpumesh/grpc/NativeBridge");
  jclass sink = env->FindClass("io/dpumesh/grpc/NativeBridge$StreamSink");
  if (bridge == nullptr || sink == nullptr) return JNI_ERR;
  g_bridge = static_cast<jclass>(env->NewGlobalRef(bridge));
  g_on_accept = env->GetStaticMethodID(bridge, "onAccept", "(J)V");
  g_on_connect = env->GetStaticMethodID(bridge, "onConnect", "(JJILjava/lang/String;)V");
  g_allocate = env->GetMethodID(sink, "allocate", "(I)Ljava/nio/ByteBuffer;");
  g_received = env->GetMethodID(sink, "received", "(I)Z");
  g_writable = env->GetMethodID(sink, "writable", "()V");
  g_eof = env->GetMethodID(sink, "eof", "()V");
  g_error = env->GetMethodID(sink, "error", "(ILjava/lang/String;)V");
  g_released = env->GetMethodID(sink, "released", "()V");
  if (Failed(env)) return JNI_ERR;
  return JNI_VERSION_1_8;
}

JNIEXPORT jlong JNICALL Java_io_dpumesh_grpc_NativeBridge_nativeOpen(JNIEnv* env, jclass,
                                                                     jstring library) {
  if (!Load(env, library)) return 0;
  char err[256] = {0};
  dms_runtime* rt = g_api.runtime_open(err, sizeof(err));
  if (rt == nullptr) {
    ThrowIo(env, std::string("DPUMesh runtime: ") + err);
    return 0;
  }
  return reinterpret_cast<jlong>(rt);
}

JNIEXPORT void JNICALL Java_io_dpumesh_grpc_NativeBridge_nativeClose(JNIEnv*, jclass, jlong rt) {
  g_api.runtime_close(reinterpret_cast<dms_runtime*>(rt));
}

JNIEXPORT jint JNICALL Java_io_dpumesh_grpc_NativeBridge_nativePostMax(JNIEnv*, jclass,
                                                                       jlong rt) {
  return static_cast<jint>(g_api.runtime_post_max(reinterpret_cast<dms_runtime*>(rt)));
}

JNIEXPORT jint JNICALL Java_io_dpumesh_grpc_NativeBridge_nativeListen(JNIEnv*, jclass, jlong rt,
                                                                      jboolean enabled) {
  return g_api.listen(reinterpret_cast<dms_runtime*>(rt), enabled ? OnAccept : nullptr,
                      nullptr);
}

JNIEXPORT void JNICALL Java_io_dpumesh_grpc_NativeBridge_nativeConnect(JNIEnv* env, jclass,
                                                                       jlong rt, jstring service,
                                                                       jlong request) {
  const char* chars = env->GetStringUTFChars(service, nullptr);
  g_api.connect(reinterpret_cast<dms_runtime*>(rt), chars, OnConnect,
                reinterpret_cast<void*>(request));
  env->ReleaseStringUTFChars(service, chars);
}

JNIEXPORT jlong JNICALL Java_io_dpumesh_grpc_NativeBridge_nativeBind(JNIEnv* env, jclass,
                                                                     jlong stream, jobject sink) {
  auto* s = new JStream;
  s->handle = reinterpret_cast<dms_stream*>(stream);
  s->sink = env->NewGlobalRef(sink);
  const int rc = g_api.stream_bind(s->handle, &kCallbacks, s);
  if (rc != 0) {
    env->DeleteGlobalRef(s->sink);
    delete s;
    g_api.stream_abort(reinterpret_cast<dms_stream*>(stream));
    ThrowIo(env, "dms_stream_bind failed: " + std::string(std::strerror(-rc)));
    return 0;
  }
  return reinterpret_cast<jlong>(s);
}

JNIEXPORT jint JNICALL Java_io_dpumesh_grpc_NativeBridge_nativeWriteDirect(
    JNIEnv* env, jclass, jlong js, jobject buffer, jint offset, jint len) {
  auto* s = reinterpret_cast<JStream*>(js);
  auto* bytes = static_cast<char*>(env->GetDirectBufferAddress(buffer));
  if (bytes == nullptr) return -EINVAL;
  return static_cast<jint>(g_api.stream_write(s->handle, bytes + offset, len));
}

JNIEXPORT jint JNICALL Java_io_dpumesh_grpc_NativeBridge_nativeWriteArray(
    JNIEnv* env, jclass, jlong js, jbyteArray array, jint offset, jint len) {
  auto* s = reinterpret_cast<JStream*>(js);
  void* bytes = env->GetPrimitiveArrayCritical(array, nullptr);
  if (bytes == nullptr) return -ENOMEM;
  const ssize_t n = g_api.stream_write(s->handle, static_cast<char*>(bytes) + offset, len);
  env->ReleasePrimitiveArrayCritical(array, bytes, JNI_ABORT);
  return static_cast<jint>(n);
}

JNIEXPORT void JNICALL Java_io_dpumesh_grpc_NativeBridge_nativeResume(JNIEnv*, jclass, jlong js) {
  g_api.stream_resume(reinterpret_cast<JStream*>(js)->handle);
}

// The caller never uses `js` again: its released() upcall, which frees it,
// may run inside this call.
JNIEXPORT void JNICALL Java_io_dpumesh_grpc_NativeBridge_nativeEnd(JNIEnv*, jclass, jlong js,
                                                                   jboolean abort) {
  auto* s = reinterpret_cast<JStream*>(js);
  if (s->ended.exchange(true)) return;
  dms_stream* handle = s->handle;
  if (abort) g_api.stream_abort(handle);
  else g_api.stream_close(handle);
}

JNIEXPORT void JNICALL Java_io_dpumesh_grpc_NativeBridge_nativeAbortUnbound(JNIEnv*, jclass,
                                                                            jlong stream) {
  g_api.stream_abort(reinterpret_cast<dms_stream*>(stream));
}

}  // extern "C"
