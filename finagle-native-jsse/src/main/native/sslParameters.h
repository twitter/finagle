/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

#include <jni.h>

#ifndef _SSLPARAMETERS_H
#define _SSLPARAMETERS_H

#ifdef __cplusplus
extern "C" {
#endif

// Protocol flags - these correspond to the flags used in SSLParameters.java
#define PROTOCOL_SSLv2 1
#define PROTOCOL_SSLv3 2
#define PROTOCOL_TLSv1 4

// Client authentication flags - these correspond to the flags used in SSLParameters.java
#define NO_CLIENT_AUTH 1
#define REQUEST_CLIENT_AUTH 2
#define REQUIRE_CLIENT_AUTH 4

JNIEXPORT jobjectArray JNICALL Java_org_apache_harmony_xnet_provider_jsse_SSLParameters_initialiseDefaults
  (JNIEnv *, jclass);
JNIEXPORT jlong JNICALL Java_org_apache_harmony_xnet_provider_jsse_SSLParameters_initialiseContext
  (JNIEnv *, jclass, jstring, jstring);
JNIEXPORT void JNICALL Java_org_apache_harmony_xnet_provider_jsse_SSLParameters_setEnabledProtocolsImpl
  (JNIEnv *, jclass, jlong, jlong, jint);
JNIEXPORT void JNICALL Java_org_apache_harmony_xnet_provider_jsse_SSLParameters_setClientAuthImpl
  (JNIEnv *, jclass, jlong, jlong, jshort);
JNIEXPORT void JNICALL Java_org_apache_harmony_xnet_provider_jsse_SSLParameters_setEnabledCipherSuitesImpl
  (JNIEnv *, jclass, jlong, jlong, jobjectArray);


#ifdef __cplusplus
}
#endif

#endif

