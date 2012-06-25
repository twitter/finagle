#!/usr/local/bin/thrift --cpp --php

##  Copyright (c) 2007-2008 Facebook
##
##  Licensed under the Apache License, Version 2.0 (the "License");
##  you may not use this file except in compliance with the License.
##  You may obtain a copy of the License at
##
##      http://www.apache.org/licenses/LICENSE-2.0
##
##  Unless required by applicable law or agreed to in writing, software
##  distributed under the License is distributed on an "AS IS" BASIS,
##  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
##  See the License for the specific language governing permissions and
##  limitations under the License.
##
## See accompanying file LICENSE or visit the Scribe site at:
## http://developers.facebook.com/scribe/

namespace cpp scribe.thrift
namespace java com.twitter.finagle.zipkin.thrift

enum ResultCode
{
  OK,
  TRY_LATER
}

struct LogEntry
{
  1:  string category,
  2:  string message
}

service scribe
{
  ResultCode Log(1: list<LogEntry> messages);
}
