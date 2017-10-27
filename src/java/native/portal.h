#pragma once
#include <jni.h>
#include <cassert>
#include <sstream>
#include <zlog/slice.h>
#include "cruzdb/db.h"

template<class PTR, class DERIVED> class CruzNativeClass {
 public:
  static jclass getJClass(JNIEnv *env, const char *jclazz_name) {
    jclass jclazz = env->FindClass(jclazz_name);
    assert(jclazz != nullptr);
    return jclazz;
  }

  static jfieldID getHandleFieldID(JNIEnv *env) {
    static jfieldID fid = env->GetFieldID(
        DERIVED::getJClass(env), "nativeHandle_", "J");
    assert(fid != nullptr);
    return fid;
  }

  static PTR getHandle(JNIEnv *env, jobject jobj) {
    return reinterpret_cast<PTR>(
        env->GetLongField(jobj, getHandleFieldID(env)));
  }

  static void setHandle(JNIEnv *env, jobject jobj, PTR ptr) {
    env->SetLongField(jobj, getHandleFieldID(env),
        reinterpret_cast<jlong>(ptr));
  }
};

class CruzDBJni : public CruzNativeClass<cruzdb::DB*, CruzDBJni> {
 public:
  static jclass getJClass(JNIEnv *env) {
    return CruzNativeClass::getJClass(env, "org/cruzdb/CruzDB");
  }
};

template<class DERIVED>
class CruzJavaException {
 public:
  static jclass getJClass(JNIEnv *env, const char *jclazz_name) {
    jclass jclazz = env->FindClass(jclazz_name);
    assert(jclazz != nullptr);
    return jclazz;
  }

  static void ThrowNew(JNIEnv *env, std::string cmsg) {
    jstring msg = env->NewStringUTF(cmsg.c_str());
    ThrowNew(env, msg);
  }

  static void ThrowNew(JNIEnv *env, int errcode) {
    if (!errcode)
      return;
    std::stringstream err;
    err << "error: errno=" << errcode;
    jstring msg = env->NewStringUTF(err.str().c_str());
    ThrowNew(env, msg);
  }

  static void ThrowNew(JNIEnv *env, jstring msg) {
    static jmethodID mid = env->GetMethodID(DERIVED::getJClass(env),
        "<init>", "(Ljava/lang/String;)V");
    assert(mid != nullptr);
    env->Throw((jthrowable)env->NewObject(DERIVED::getJClass(env),
          mid, msg));
  }
};

class CruzDBExceptionJni : public CruzJavaException<CruzDBExceptionJni> {
 public:
  static jclass getJClass(JNIEnv* env) {
    return CruzJavaException::getJClass(env, "org/cruzdb/CruzDBException");
  }
};
