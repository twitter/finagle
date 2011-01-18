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
#include <unistd.h>

#include "jni.h"

#include "openssl/bio.h"
#include "openssl/ssl.h"
#include "openssl/err.h"

#include "addr.h"
#include "sslEngine.h"

typedef struct {
  BIO *bio;
  BIO *bio_io;
} _sslengine;

static jobject handshake_need_wrap, handshake_need_unwrap, handshake_finished, handshake_not_handshaking;
static jobject engine_buffer_overflow, engine_buffer_underflow, engine_closed, engine_ok;

int check_ssl_error(JNIEnv *env, int state) {
    jclass exception;
    switch(state) {
    case SSL_ERROR_SYSCALL:
    case SSL_ERROR_SSL:
      exception = (*env)->FindClass(env, "javax/net/ssl/SSLHandshakeException");
      (*env)->ThrowNew(env, exception, ERR_reason_error_string(ERR_get_error()));
      return 1;
    }
    return 0;
}

JNIEXPORT void JNICALL Java_org_apache_harmony_xnet_provider_jsse_SSLEngineImpl_initImpl
  (JNIEnv *env, jclass clazz) {
    jclass class;
    jfieldID fieldID;
    // initialise handshake status enum
    class = (*env)->FindClass(env, "javax/net/ssl/SSLEngineResult$HandshakeStatus");
    fieldID = (*env)->GetStaticFieldID(env, class, "NEED_WRAP", "Ljavax/net/ssl/SSLEngineResult$HandshakeStatus;");
    handshake_need_wrap = (*env)->NewGlobalRef(env, (*env)->GetStaticObjectField(env, class, fieldID));
    fieldID = (*env)->GetStaticFieldID(env, class, "NEED_UNWRAP", "Ljavax/net/ssl/SSLEngineResult$HandshakeStatus;");
    handshake_need_unwrap = (*env)->NewGlobalRef(env, (*env)->GetStaticObjectField(env, class, fieldID));
    fieldID = (*env)->GetStaticFieldID(env, class, "FINISHED", "Ljavax/net/ssl/SSLEngineResult$HandshakeStatus;");
    handshake_finished = (*env)->NewGlobalRef(env, (*env)->GetStaticObjectField(env, class, fieldID));
    fieldID = (*env)->GetStaticFieldID(env, class, "NOT_HANDSHAKING", "Ljavax/net/ssl/SSLEngineResult$HandshakeStatus;");
    handshake_not_handshaking = (*env)->NewGlobalRef(env, (*env)->GetStaticObjectField(env, class, fieldID));

    // initialise engine status enum
    class = (*env)->FindClass(env, "javax/net/ssl/SSLEngineResult$Status");
    fieldID = (*env)->GetStaticFieldID(env, class, "BUFFER_OVERFLOW", "Ljavax/net/ssl/SSLEngineResult$Status;");
    engine_buffer_overflow = (*env)->NewGlobalRef(env, (*env)->GetStaticObjectField(env, class, fieldID));
    fieldID = (*env)->GetStaticFieldID(env, class, "BUFFER_UNDERFLOW", "Ljavax/net/ssl/SSLEngineResult$Status;");
    engine_buffer_underflow = (*env)->NewGlobalRef(env, (*env)->GetStaticObjectField(env, class, fieldID));
    fieldID = (*env)->GetStaticFieldID(env, class, "CLOSED", "Ljavax/net/ssl/SSLEngineResult$Status;");
    engine_closed = (*env)->NewGlobalRef(env, (*env)->GetStaticObjectField(env, class, fieldID));
    fieldID = (*env)->GetStaticFieldID(env, class, "OK", "Ljavax/net/ssl/SSLEngineResult$Status;");
    engine_ok = (*env)->NewGlobalRef(env, (*env)->GetStaticObjectField(env, class, fieldID));
}

JNIEXPORT jlong JNICALL Java_org_apache_harmony_xnet_provider_jsse_SSLEngineImpl_initSSL
  (JNIEnv *env, jclass clazz, jlong context) {
    SSL *ssl;
    ssl = SSL_new(jlong2addr(SSL_CTX, context));
    return addr2jlong(ssl);
}

JNIEXPORT jlong JNICALL Java_org_apache_harmony_xnet_provider_jsse_SSLEngineImpl_initSSLEngine
  (JNIEnv *env, jclass clazz, jlong jssl) {
    _sslengine *sslengine;
    SSL *ssl = jlong2addr(SSL, jssl);
    BIO *bio, *bio_in, *bio_out;
    sslengine = malloc(sizeof(_sslengine));
    bio = BIO_new(BIO_f_ssl());
    BIO_set_ssl(bio, ssl, BIO_NOCLOSE);
    // create a bio pair
    BIO_new_bio_pair(&bio_in, 0, &bio_out, 0);
    BIO_get_ssl(bio, &ssl);
    SSL_set_bio(ssl, bio_in, bio_in);
    sslengine->bio = bio;
    sslengine->bio_io = bio_out;
    return addr2jlong(sslengine);
}

JNIEXPORT jobject JNICALL Java_org_apache_harmony_xnet_provider_jsse_SSLEngineImpl_acceptImpl
  (JNIEnv *env, jclass clazz, jlong jssl) {
    SSL *ssl = jlong2addr(SSL, jssl);
    int ret;

    // Put our SSL into accept state
    SSL_set_accept_state(ssl);
    // Start the client handshake
    ret = SSL_do_handshake(ssl);

    if (check_ssl_error(env, SSL_get_error(ssl, ret))) {
        return NULL;
    }

    return handshake_need_unwrap;
}


JNIEXPORT jobject JNICALL Java_org_apache_harmony_xnet_provider_jsse_SSLEngineImpl_wrapImpl
  (JNIEnv *env, jclass clazz, jlong jssl, jlong jsslengine, jlong src_address, int src_len,
  jlong dst_address, int dst_len) {
    _sslengine *sslengine = jlong2addr(_sslengine, jsslengine);
    BIO *bio = sslengine->bio;
    BIO *bio_io = sslengine->bio_io;
    SSL *ssl = jlong2addr(SSL, jssl);
    int write_result = 0, read_result = 0, shutdownState = 0;
    jobject handshake_state = NULL, engine_state = engine_ok, result = NULL;
    jclass result_class;
    jmethodID result_constructor;
    jbyte *src_buffer = jlong2addr(jbyte, src_address);
    jbyte *dst_buffer = jlong2addr(jbyte, dst_address);
    int initial_init_state, init_state;

    initial_init_state = SSL_in_init(ssl);

    // write input data
    if (BIO_ctrl_get_write_guarantee(bio) >= (size_t)src_len) {
        write_result = BIO_write(bio, (const void *)src_buffer, (int)src_len);
    } else {
        write_result = 0;
    }

    if (write_result > 0) {
        // wrote some data so must not be handshaking
        handshake_state = handshake_not_handshaking;
    } else {
        write_result = 0;
        handshake_state = handshake_need_unwrap;
    }

    // Check if close_notify has been sent or received
    shutdownState = SSL_get_shutdown(ssl);
    if (shutdownState) {
        engine_state = engine_closed;
    }

    // read output data
    // if the destination buffer is too small
    if (BIO_ctrl_pending(bio_io) > (size_t)dst_len) {
        engine_state = engine_buffer_overflow;
    } else {
        read_result = BIO_read(bio_io, dst_buffer, dst_len);
    }

    if (read_result < 0) {
        // change state?
        read_result = 0;
    }

    init_state = SSL_in_init(ssl);

    // if not in SSL init state
    if (!init_state) {
        // if we were in init state when we entered this function

        if (initial_init_state) {
            handshake_state = handshake_finished;
        } else {
            handshake_state = handshake_not_handshaking;
        }
    }

    // construct return object
    // XXX - experiment with making the lookup of these classes static?
    result_class = (*env)->FindClass(env, "javax/net/ssl/SSLEngineResult");
    result_constructor = (*env)->GetMethodID(env, result_class, "<init>",
        "(Ljavax/net/ssl/SSLEngineResult$Status;Ljavax/net/ssl/SSLEngineResult$HandshakeStatus;II)V");
    result = (*env)->NewObject(env, result_class, result_constructor,
        engine_state, handshake_state, write_result, read_result);
    return result;
}

JNIEXPORT jobject JNICALL Java_org_apache_harmony_xnet_provider_jsse_SSLEngineImpl_unwrapImpl
  (JNIEnv *env, jclass clazz, jlong jssl, jlong jsslengine, jlong src_address, int src_len,
  jlong dst_address, int dst_len) {
    _sslengine *sslengine = jlong2addr(_sslengine, jsslengine);
    BIO *bio = sslengine->bio;
    BIO *bio_io = sslengine->bio_io;
    SSL *ssl = jlong2addr(SSL, jssl);
    int write_result = 0, read_result = 0, shutdownState = 0;
    jobject handshake_state = handshake_not_handshaking, engine_state = engine_ok, result = NULL;
    jclass result_class;
    jmethodID result_constructor;
    jbyte *src_buffer = jlong2addr(jbyte, src_address);
    jbyte *dst_buffer = jlong2addr(jbyte, dst_address);
    int initial_init_state;
    int read_pending = 0;

    initial_init_state = SSL_in_init(ssl);

    // write input data
    write_result = BIO_write(bio_io, (const void *)src_buffer, (int)src_len);
    if (write_result < 0) {
        // change state?
        write_result = 0;
    }

    // TODO check for errors
    // read output data
    read_pending = BIO_ctrl_pending(bio);
    if (read_pending != 0) {
        if (read_pending > dst_len) {
            engine_state = engine_buffer_overflow;
        } else {
            read_result = SSL_read(ssl, dst_buffer, dst_len);

            // Check if close_notify has been sent or received
            shutdownState = SSL_get_shutdown(ssl);
            if (shutdownState) {
                engine_state = engine_closed;
            }

            if (read_result < 0) {
                read_result = 0;
            }
        }
        // if not in SSL init state
        if (SSL_in_init(ssl)) {
            handshake_state = handshake_need_wrap;
        } else {
            // if we were in init state when we entered this function
            if (initial_init_state) {
                /* originally, handshake_finished. */
                handshake_state = handshake_need_wrap;
            } else {
                handshake_state = handshake_not_handshaking;
            }
        }
    }

    // construct return object
    result_class = (*env)->FindClass(env, "javax/net/ssl/SSLEngineResult");
    result_constructor = (*env)->GetMethodID(env, result_class, "<init>",
        "(Ljavax/net/ssl/SSLEngineResult$Status;Ljavax/net/ssl/SSLEngineResult$HandshakeStatus;II)V");
    result = (*env)->NewObject(env, result_class, result_constructor,
        engine_state, handshake_state, write_result, read_result);
    return result;
}

void shutdownImpl(JNIEnv *env, SSL *ssl) {
    int ret = SSL_shutdown(ssl);
    if (ret == -1) {
        jclass exception = (*env)->FindClass(env, "javax/net/ssl/SSLException");
        (*env)->ThrowNew(env, exception, ERR_reason_error_string(ERR_get_error()));
    }

    if (ret == 0) {
        ret = SSL_shutdown(ssl);
        if (((ret == -1) && (SSL_get_error(ssl, ret) != SSL_ERROR_WANT_READ)) || (ret == 0)) {
            jclass exception = (*env)->FindClass(env, "javax/net/ssl/SSLException");
            (*env)->ThrowNew(env, exception, ERR_reason_error_string(ERR_get_error()));
        }
    }
}

JNIEXPORT void JNICALL Java_org_apache_harmony_xnet_provider_jsse_SSLEngineImpl_shutdownImpl
  (JNIEnv *env, jclass clazz, jlong jssl) {
    SSL *ssl = jlong2addr(SSL, jssl);
    shutdownImpl(env, ssl);
}

JNIEXPORT void JNICALL Java_org_apache_harmony_xnet_provider_jsse_SSLEngineImpl_closeInboundImpl
  (JNIEnv *env, jclass clazz, jlong jsslengine) {
    _sslengine *sslengine = jlong2addr(_sslengine, jsslengine);
    BIO_shutdown_wr(sslengine->bio_io);
}

