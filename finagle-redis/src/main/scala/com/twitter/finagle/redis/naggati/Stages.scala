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

import org.jboss.netty.buffer.ChannelBuffer
import scala.language.implicitConversions

object Stages {
  /**
   * Generate a Stage from a code block.
   */
  def stage(f: ChannelBuffer => NextStep): Stage = new Stage {
    def apply(buffer: ChannelBuffer) = f(buffer)
  }

  /**
   * Wrap a Stage. The wrapped stage will be regenerated on each call.
   */
  def proxy(stage: => Stage): Stage = new Stage {
    def apply(buffer: ChannelBuffer) = stage.apply(buffer)
  }

  /**
   * Allow a decoder to return a Stage when we expected a NextStep.
   */
  implicit def stageToNextStep(stage: Stage): NextStep = GoToStage(stage)

  def emit(obj: AnyRef): NextStep = Emit(obj)

  /**
   * Ensure that a certain number of bytes is buffered before executing the next step, calling
   * `getCount` each time new data arrives, to recompute the total number of bytes desired.
   */
  def ensureBytesDynamic(getCount: => Int)(process: ChannelBuffer => NextStep): Stage = proxy {
    ensureBytes(getCount)(process)
  }

  /**
   * Ensure that a certain number of bytes is buffered before executing the * next step.
   */
  def ensureBytes(count: Int)(process: ChannelBuffer => NextStep): Stage = stage { buffer =>
    if (buffer.readableBytes < count) {
      Incomplete
    } else {
      process(buffer)
    }
  }

  /**
   * Read a certain number of bytes into a byte buffer and pass that buffer to the next step in
   * processing. `getCount` is called each time new data arrives, to recompute * the total number of
   * bytes desired.
   */
  def readBytesDynamic(getCount: => Int)(process: Array[Byte] => NextStep) = proxy {
    readBytes(getCount)(process)
  }

  /**
   * Read `count` bytes into a byte buffer and pass that buffer to the next step in processing.
   */
  def readBytes(count: Int)(process: Array[Byte] => NextStep) = stage { buffer =>
    if (buffer.readableBytes < count) {
      Incomplete
    } else {
      val bytes = new Array[Byte](count)
      buffer.readBytes(bytes)
      process(bytes)
    }
  }

  /**
   * Read bytes until a delimiter is present. The number of bytes up to and including the delimiter
   * is passed to the next processing step. `getDelimiter` is called each time new data arrives.
   */
  def ensureDelimiterDynamic(getDelimiter: => Byte)(process: (Int, ChannelBuffer) => NextStep) = proxy {
    ensureDelimiter(getDelimiter)(process)
  }

  /**
   * Read bytes until a delimiter is present. The number of bytes up to and including the delimiter
   * is passed to the next processing step.
   */
  def ensureDelimiter(delimiter: Byte)(process: (Int, ChannelBuffer) => NextStep) = stage { buffer =>
    val n = buffer.bytesBefore(delimiter)
    if (n < 0) {
      Incomplete
    } else {
      process(n + 1, buffer)
    }
  }

  /**
   * Read bytes until a delimiter is present, and pass a buffer containing the bytes up to and
   * including the delimiter to the next processing step. `getDelimiter` is called each time new
   * data arrives.
   */
  def readToDelimiterDynamic(getDelimiter: => Byte)(process: Array[Byte] => NextStep) = proxy {
    readToDelimiter(getDelimiter)(process)
  }

  /**
   * Read bytes until a delimiter is present, and pass a buffer containing the bytes up to and
   * including the delimiter to the next processing step.
   */
  def readToDelimiter(delimiter: Byte)(process: (Array[Byte]) => NextStep) = stage { buffer =>
    ensureDelimiter(delimiter) { (n, buffer) =>
      val byteBuffer = new Array[Byte](n)
      buffer.readBytes(byteBuffer)
      process(byteBuffer)
    }
  }

  /**
   * Read a line, terminated by LF or CR/LF, and pass that line as a string to the next processing
   * step.
   *
   * @param removeLF true if the LF or CRLF should be stripped from the string before passing it on
   * @param encoding byte-to-character encoding to use
   */
  def readLine(removeLF: Boolean, encoding: String)(process: String => NextStep): Stage = {
    ensureDelimiter('\n'.toByte) { (n, buffer) =>
      val end = if ((n > 1) && (buffer.getByte(buffer.readerIndex + n - 2) == '\r'.toByte)) {
        n - 2
      } else {
        n - 1
      }
      val byteBuffer = new Array[Byte](n)
      buffer.readBytes(byteBuffer)
      process(new String(byteBuffer, 0, (if (removeLF) end else n), encoding))
    }
  }

  /**
   * Read a line, terminated by LF or CRLF, and pass that line as a string (decoded using
   * UTF-8) to the next processing step.
   *
   * @param removeLF true if the LF or CRLF should be stripped from the string before passing it on
   */
  def readLine(removeLF: Boolean)(process: String => NextStep): Stage =
    readLine(removeLF, "UTF-8")(process)

  /**
   * Read a line, terminated by LF or CRLF, and pass that line as a string (decoded using
   * UTF-8, with the line terminators stripped) to the next processing step.
   */
  def readLine(process: String => NextStep): Stage = readLine(true, "UTF-8")(process)
}
