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

#ifndef _CIPHERLIST_H
#define _CIPHERLIST_H

#define SSLv2_CIPHER_COUNT 7
#define SSLv3_CIPHER_COUNT 21
#define TLSv1_CIPHER_COUNT 50

char** getTLSv1SpecNames();
char** getTLSv1OpenSSLNames();
char** getSSLv3SpecNames();
char** getSSLv3OpenSSLNames();
char** getSSLv2SpecNames();
char** getSSLv2OpenSSLNames();

#endif
