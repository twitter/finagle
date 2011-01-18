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

#include "jsse_rand.h"

#include <stdio.h>
#include "jni.h"
#include "openssl/rand.h"

void randSeed(const void *buf, int num);
int  randBytes(unsigned char *buf, int num);
void randCleanup(void);
void randAdd(const void *buf, int num, double entropy);
int randPseudoBytes(unsigned char *buf, int num);
int randStatus(void);

struct callbackFunctions_struct {
    JavaVM *javaVM;
    void (*origRandSeed)(const void *buf, int num);
    int  (*origRandBytes)(unsigned char *buf, int num);
    void (*origRandAdd)(const void *buf, int num, double entropy);
    int (*origRandPseudoBytes)(unsigned char *buf, int num);
    jclass rngClass;
    jmethodID randSeedMethod;
    jmethodID randBytesMethod;
    jmethodID randAddMethod;
    jmethodID randPseudoMethod;
};

struct callbackFunctions_struct *callbacks;

void initialiseRandMethod(JNIEnv *env) {
    const RAND_METHOD *origRandMethod;
    RAND_METHOD *newRandMethod;

    origRandMethod = RAND_get_rand_method();
    callbacks = malloc(sizeof(struct callbackFunctions_struct));
    (*env)->GetJavaVM(env, &callbacks->javaVM);
    callbacks->origRandSeed = origRandMethod->seed;
    callbacks->origRandBytes = origRandMethod->bytes;
    callbacks->origRandAdd = origRandMethod->add;
    callbacks->origRandPseudoBytes = origRandMethod->pseudorand;

    // In each of the following cases if the return value is NULL an exception should already have been
    // thrown, so just return
    callbacks->rngClass = (*env)->FindClass(env, "org/apache/harmony/xnet/provider/jsse/RNGHandler");
    if (!callbacks->rngClass) return;

    callbacks->randSeedMethod = (*env)->GetStaticMethodID(env, callbacks->rngClass, "randSeed", "([B)I");
    if (!callbacks->randSeedMethod) return;
    callbacks->randBytesMethod = (*env)->GetStaticMethodID(env, callbacks->rngClass, "randBytes", "(I)[B");
    if (!callbacks->randBytesMethod) return;
    callbacks->randAddMethod = (*env)->GetStaticMethodID(env, callbacks->rngClass, "randAdd", "([B)I");
    if (!callbacks->randAddMethod) return;
    callbacks->randPseudoMethod = (*env)->GetStaticMethodID(env, callbacks->rngClass, "randPseudoBytes", "(I)[B");
    if (!callbacks->randPseudoMethod) return;
    
    newRandMethod = malloc(sizeof(RAND_METHOD));
    newRandMethod->seed = &randSeed;
    newRandMethod->bytes = &randBytes;
    newRandMethod->cleanup = &randCleanup;
    newRandMethod->add = &randAdd;
    newRandMethod->pseudorand = &randPseudoBytes;
    newRandMethod->status = &randStatus;

    // TODO: Check for error returns here
    RAND_set_rand_method(newRandMethod);
}

void randSeed(const void *buf, int num) {
    JNIEnv *env;
    int ret;
    jbyteArray seedBuffer;

    ret = (*callbacks->javaVM)->GetEnv(callbacks->javaVM, (void**)&env, JNI_VERSION_1_4);
    if (ret) {
        // TODO: throw appropriate exception
        return;
    }

    seedBuffer = (*env)->NewByteArray(env, (jsize)num);
    if ((*env)->ExceptionOccurred(env)) {
        return;
    }

    (*env)->SetByteArrayRegion(env, seedBuffer, 0, (jsize)num, (jbyte*)buf);
    if (!seedBuffer) {
        return;
    }

    ret = (*env)->CallStaticIntMethod(env, callbacks->rngClass, callbacks->randSeedMethod, seedBuffer);
    if (ret) {
        // No SecureRandom implementation was set for this context - just call the OpenSSL default function
        (*callbacks->origRandSeed)(buf, num);
    }
}

int randBytes(unsigned char *buf, int num) {
    JNIEnv *env;
    int ret;
    jbyteArray randBuffer;


    ret = (*callbacks->javaVM)->GetEnv(callbacks->javaVM, (void**)&env, JNI_VERSION_1_4);
    if (ret) {
        // TODO: throw appropriate exception and return
        return 0;
    }

    randBuffer = (jbyteArray)(*env)->CallStaticObjectMethod(env, callbacks->rngClass, callbacks->randBytesMethod, (jint)num);
    if (!randBuffer) {
        // No SecureRandom implementation was set for this context - just call the OpenSSL default function
        ret = (*callbacks->origRandBytes)(buf, num);
        if (!ret) {
            // TODO: throw appropriate exception here
            return 0;
        }
    } else {
        (*env)->GetByteArrayRegion(env, randBuffer, 0, (jint)num, (jbyte*)buf);
    }

    return 1;
}

void randCleanup() {
    // Do nothing and return
    return;
}

void randAdd(const void *buf, int num, double entropy) {
    JNIEnv *env;
    int ret;
    jbyteArray seedBuffer;

    ret = (*callbacks->javaVM)->GetEnv(callbacks->javaVM, (void**)&env, JNI_VERSION_1_4);
    if (ret) {
        // TODO: throw appropriate exception
        return;
    }

    seedBuffer = (*env)->NewByteArray(env, (jsize)num);
    if ((*env)->ExceptionOccurred(env)) {
        return;
    }

    (*env)->SetByteArrayRegion(env, seedBuffer, 0, (jsize)num, (jbyte*)buf);
    if (!seedBuffer) {
        return;
    }

    ret = (*env)->CallStaticIntMethod(env, callbacks->rngClass, callbacks->randAddMethod, seedBuffer);
    if (ret) {
        // No SecureRandom implementation was set for this context - just call the OpenSSL default function
        (*callbacks->origRandAdd)(buf, num, entropy);
    }
}

int randPseudoBytes(unsigned char *buf, int num) {
    JNIEnv *env;
    int ret;
    jbyteArray randBuffer;

    ret = (*callbacks->javaVM)->GetEnv(callbacks->javaVM, (void**)&env, JNI_VERSION_1_4);
    if (ret) {
        // TODO: throw appropriate exception and return
        return 0;
    }

    randBuffer = (jbyteArray)(*env)->CallStaticObjectMethod(env, callbacks->rngClass, callbacks->randPseudoMethod, (jint)num);
    if (!randBuffer) {
        // No SecureRandom implementation was set for this context - just call the OpenSSL default function
        ret = (*callbacks->origRandPseudoBytes)(buf, num);
        if (!ret) {
            // TODO: throw appropriate exception here
            return 0;
        }
    } else {
        (*env)->GetByteArrayRegion(env, randBuffer, 0, (jint)num, (jbyte*)buf);
    }

    return 1;
}

int randStatus() {
    // Do nothing and return success
    return 1;
}
