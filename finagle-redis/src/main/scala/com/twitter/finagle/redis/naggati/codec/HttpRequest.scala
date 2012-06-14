/*
 * Copyright 2010 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitter.finagle.redis.naggati
package codec

import Stages._

case class RequestLine(method: String, resource: String, version: String)
case class HeaderLine(name: String, value: String)
case class HttpRequest(request: RequestLine, headers: List[HeaderLine], body: Array[Byte])

object HttpRequest {
  def codec(bytesReadCounter: Int => Unit, bytesWrittenCounter: Int => Unit) =
    new Codec(read, Codec.NONE, bytesReadCounter, bytesWrittenCounter)

  def codec() = new Codec(read, Codec.NONE)

  val read = readLine(true, "UTF-8") { line =>
    line.split(' ').toList match {
      case method :: resource :: version :: Nil =>
        val requestLine = RequestLine(method, resource, version)
        readHeader(requestLine, Nil)
      case _ =>
        throw new ProtocolError("Malformed request line: " + line)
    }
  }

  def readHeader(requestLine: RequestLine, headers: List[HeaderLine]): Stage = {
    readLine(true, "UTF-8") { line =>
      if (line == "") {
        // end of headers
        val contentLength = headers.find { _.name == "content-length" }.map { _.value.toInt }.getOrElse(0)
        readBytes(contentLength) { data =>
          emit(HttpRequest(requestLine, headers.reverse, data))
        }
      } else if (line.length > 0 && (line.head == ' ' || line.head == '\t')) {
        // continuation line
        if (headers.size == 0) {
          throw new ProtocolError("Malformed header line: " + line)
        }
        val newHeaderLine = HeaderLine(headers.head.name, headers.head.value + " " + line.trim)
        readHeader(requestLine, newHeaderLine :: headers.drop(1))
      } else {
        val newHeaderLine = line.split(':').toList match {
          case name :: value :: Nil =>
            HeaderLine(name.trim.toLowerCase, value.trim)
          case _ =>
            throw new ProtocolError("Malformed header line: " + line)
        }
        readHeader(requestLine, newHeaderLine :: headers)
      }
    }
  }
}
