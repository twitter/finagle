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

#if !defined(addr_h)
#define addr_h

#ifdef __x86_64
#define jlong2addr(a, x) ((a *)((int64_t)(x)))
#define addr2jlong(x) ((jlong)((int64_t)(x)))
#else
#warning This code is not well tested on the 32-bit memory model
#warning Get a real computer
#define jlong2addr(a, x) ((a *)((int32_t)(x)))
#define addr2jlong(x) ((jlong)((int32_t)(x)))
#endif // __x86_64

#endif // addr_h
