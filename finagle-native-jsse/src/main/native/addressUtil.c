#include <stdint.h>
#include <jni.h>

#include "addressUtil.h"
#include "addr.h"

JNIEXPORT jlong JNICALL Java_org_apache_harmony_xnet_provider_jsse_AddressUtil_getDirectBufferAddressNative
  (JNIEnv *env, jclass klass, jobject buf) {
  jlong address = addr2jlong((*env)->GetDirectBufferAddress(env, buf));
  return address;
}
