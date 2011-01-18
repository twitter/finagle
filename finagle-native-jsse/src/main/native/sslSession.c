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

#include <stdio.h>

#include "jni.h"

#include "openssl/bio.h"
#include "openssl/ssl.h"
#include "openssl/err.h"

#include "addr.h"
#include "cipherList.h"
#include "sslSession.h"

JNIEXPORT jlong JNICALL Java_org_apache_harmony_xnet_provider_jsse_SSLSessionImpl_initialiseSession
  (JNIEnv *env, jobject object, jlong jssl)
{
    SSL *ssl = jlong2addr(SSL, jssl);
    return addr2jlong(SSL_get_session(ssl));
}

char* getSpecName(const char *cipherName, char *openSSLNames[], char *specNames[], int mappedNamesCount) {
    int i;
    for (i=0; i<mappedNamesCount; i++) {
        if (!strcmp(cipherName, openSSLNames[i])) {
            return specNames[i];
        }
    }
    return NULL;
}

JNIEXPORT jstring JNICALL Java_org_apache_harmony_xnet_provider_jsse_SSLSessionImpl_getCipherNameImpl
  (JNIEnv *env, jobject object, jlong jssl) {
    SSL *ssl = jlong2addr(SSL, jssl);
    const char *cipherName = SSL_get_cipher(ssl);
    char *protocol = SSL_get_cipher_version(ssl);
    char *specName = NULL;
    char *finalName;
   
    if (!strcmp(protocol, "TLSv1/SSLv3")) {
        // We're in either TLS or SSLv3, now find the spec name
        specName = getSpecName(cipherName, getTLSv1OpenSSLNames(), getTLSv1SpecNames(), TLSv1_CIPHER_COUNT);
        if (specName) {
            protocol = "TLSv1";
        } else {
            // Not in the TLS list, now search the SSL list
            // TODO: Lists are likely to be the same - can this case ever occur?
            specName = getSpecName(cipherName, getSSLv3OpenSSLNames(), getSSLv3SpecNames(), SSLv3_CIPHER_COUNT);
            protocol = "SSLv3";
        }
    } else if (!strcmp(protocol, "(NONE)")) {
        // Handshake not completed yet - return NULL protocol/cipher
        return (*env)->NewStringUTF(env, "NONE:SSL_NULL_WITH_NULL_NULL");
    } else {
        // SSLv2 case - protocol will already be "SSLv2", so no need to set it
        specName = getSpecName(cipherName, getSSLv2OpenSSLNames(), getSSLv2SpecNames(), SSLv2_CIPHER_COUNT);
    } 

    // finalName is "protocol:specName\0"
    // protocol length is always 5, so allocate strlen(specName) + 5 + 1 for the colon + 1 for the terminator
    finalName = malloc(strlen(specName)+7);
    strcpy(finalName, protocol);
    strcat(finalName, ":");
    strcat(finalName, specName);

    return (*env)->NewStringUTF(env, finalName);
}

JNIEXPORT jlong JNICALL Java_org_apache_harmony_xnet_provider_jsse_SSLSessionImpl_getCreationTimeImpl
  (JNIEnv *env, jobject object, jlong jsession) {
    SSL_SESSION *session = jlong2addr(SSL_SESSION, jsession);

    return (jlong)SSL_SESSION_get_time(session)*1000;
}

JNIEXPORT jobjectArray JNICALL Java_org_apache_harmony_xnet_provider_jsse_SSLSessionImpl_getPeerCertificatesImpl
  (JNIEnv *env, jobject object, jlong jssl) {
    SSL *ssl = jlong2addr(SSL, jssl);
    STACK_OF(X509) *certs;
    int certCount, i;
    jobjectArray jcerts;
    jclass byteArrayClass;

    // Get the chain of peer certificates from OpenSSL
    certs = SSL_get_peer_cert_chain(ssl);
    if (!certs) {
        return NULL;
    }

    // Get the number of certificates in the chain
    certCount = sk_num(&certs->stack);
    if (!certCount) {
        return NULL;
    }

    // Allocate an array of jbyte arrays to contain the peer certs
    byteArrayClass = (*env)->FindClass(env, "[B");
    jcerts = (*env)->NewObjectArray(env, certCount, byteArrayClass, NULL);

    for (i=0; i<certCount; i++) {
        unsigned char *certBuffer = NULL;
        jbyteArray jcertBuffer;

        // OpenSSL will automatically allocate the buffer for us because certBuffer is NULL
        int len = i2d_X509(sk_value(&certs->stack, i), &certBuffer);

        // Allocate a jbyte array for the certificate data and copy it over
        jcertBuffer = (*env)->NewByteArray(env, len);
        (*env)->SetByteArrayRegion(env, jcertBuffer, 0, len, (jbyte*)certBuffer);
        (*env)->SetObjectArrayElement(env, jcerts, i, jcertBuffer);
    }

    return jcerts;
}
