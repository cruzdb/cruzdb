#include <iostream>
#include <boost/exception/diagnostic_information.hpp>
#include <jni.h>

#include "org_cruzdb_CruzDB.h"
#include "portal.h"

static jbyteArray copyBytes(JNIEnv* env, std::string bytes) {
  const jsize jlen = static_cast<jsize>(bytes.size());

  jbyteArray jbytes = env->NewByteArray(jlen);
  if(jbytes == nullptr) {
    // exception thrown: OutOfMemoryError
    return nullptr;
  }

  env->SetByteArrayRegion(jbytes, 0, jlen,
      const_cast<jbyte*>(reinterpret_cast<const jbyte*>(bytes.c_str())));
  if(env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    env->DeleteLocalRef(jbytes);
    return nullptr;
  }

  return jbytes;
}

void Java_org_cruzdb_CruzDB_disposeInternal(JNIEnv *env, jobject jobj,
    jlong jhandle)
{
  delete reinterpret_cast<cruzdb::DB*>(jhandle);
}

void Java_org_cruzdb_CruzDB_openNative(JNIEnv *env, jobject jobj,
    jlong jdbHandle, jboolean jcreate)
{
  auto log = reinterpret_cast<zlog::Log*>(jdbHandle);

  cruzdb::DB *db;
  cruzdb::Options options;
  int ret = cruzdb::DB::Open(options, log, jcreate, &db);
  if (ret) {
    CruzDBExceptionJni::ThrowNew(env, ret);
    return;
  }

  CruzDBJni::setHandle(env, jobj, db);
}

void Java_org_cruzdb_CruzDB_put(JNIEnv *env, jobject jdb, jlong jdbHandle,
    jbyteArray jkey, jint jkeyOffset, jint jkeyLength, jbyteArray jval,
    jint jvalOffset, jint jvalLength)
{
  auto *db = reinterpret_cast<cruzdb::DB*>(jdbHandle);

  jbyte *key = new jbyte[jkeyLength];
  env->GetByteArrayRegion(jkey, jkeyOffset, jkeyLength, key);
  if (env->ExceptionCheck()) {
    delete [] key;
    return;
  }

  jbyte *value = new jbyte[jvalLength];
  env->GetByteArrayRegion(jval, jvalOffset, jvalLength, value);
  if (env->ExceptionCheck()) {
    delete [] value;
    delete [] key;
    return;
  }

  zlog::Slice key_slice(reinterpret_cast<char*>(key), jkeyLength);
  zlog::Slice value_slice(reinterpret_cast<char*>(value), jvalLength);

  auto txn = db->BeginTransaction();
  txn->Put(key_slice, value_slice);
  auto committed = txn->Commit();
  assert(committed);
  delete txn;

  delete [] value;
  delete [] key;
}

jint Java_org_cruzdb_CruzDB_get(JNIEnv *env, jobject jdb, jlong jdbHandle,
    jbyteArray jkey, jint jkeyOffset, jint jkeyLength, jbyteArray jval,
    jint jvalOffset, jint jvalLength)
{
  auto *db = reinterpret_cast<cruzdb::DB*>(jdbHandle);

  jbyte *key = new jbyte[jkeyLength];
  env->GetByteArrayRegion(jkey, jkeyOffset, jkeyLength, key);
  if (env->ExceptionCheck()) {
    delete [] key;
    return -2;
  }

  zlog::Slice key_slice(reinterpret_cast<char*>(key), jkeyLength);

  std::string value;
  int ret = db->Get(key_slice, &value);

  delete [] key;

  if (ret == -ENOENT)
    return -1;

  const jint value_length = static_cast<jint>(value.size());
  const jint length = std::min(jvalLength, value_length);
  env->SetByteArrayRegion(jval, jvalOffset, length,
      const_cast<jbyte*>(reinterpret_cast<const jbyte*>(value.c_str())));
  if (env->ExceptionCheck()) {
    return -2;
  }

  return value_length;
}

jbyteArray Java_org_cruzdb_CruzDB_get__J_3BII
  (JNIEnv *env, jobject jdb, jlong jdbHandle, jbyteArray jkey,
   jint jkeyOffset, jint jkeyLength)
{
  auto *db = reinterpret_cast<cruzdb::DB*>(jdbHandle);

  jbyte *key = new jbyte[jkeyLength];
  env->GetByteArrayRegion(jkey, jkeyOffset, jkeyLength, key);
  if (env->ExceptionCheck()) {
    delete [] key;
    return nullptr;
  }

  zlog::Slice key_slice(reinterpret_cast<char*>(key), jkeyLength);

  std::string value;
  int ret = db->Get(key_slice, &value);

  delete [] key;

  if (ret == -ENOENT)
    return nullptr;

  jbyteArray jret_value = copyBytes(env, value);
  if (jret_value == nullptr)
    return nullptr;
  return jret_value;
}

void Java_org_cruzdb_CruzDB_delete(JNIEnv *env, jobject jdb, jlong jdbHandle,
    jbyteArray jkey, jint jkeyOffset, jint jkeyLength)
{
  auto *db = reinterpret_cast<cruzdb::DB*>(jdbHandle);

  jbyte *key = new jbyte[jkeyLength];
  env->GetByteArrayRegion(jkey, jkeyOffset, jkeyLength, key);
  if(env->ExceptionCheck()) {
    delete [] key;
    return;
  }

  zlog::Slice key_slice(reinterpret_cast<char*>(key), jkeyLength);

  auto *txn = db->BeginTransaction();
  txn->Delete(key_slice);
  txn->Commit();
  delete txn;

  delete [] key;
}

jlong Java_org_cruzdb_CruzDB_iterator(JNIEnv *env, jobject jdb,
    jlong jdbHandle)
{
  auto *db = reinterpret_cast<cruzdb::DB*>(jdbHandle);
  auto *iterator = db->NewIterator();
  return reinterpret_cast<jlong>(iterator);
}

jlong Java_org_cruzdb_CruzDB_transaction(JNIEnv *env, jobject jdb,
    jlong jdbHandle)
{
  auto *db = reinterpret_cast<cruzdb::DB*>(jdbHandle);
  auto *txn = db->BeginTransaction();
  return reinterpret_cast<jlong>(txn);
}
