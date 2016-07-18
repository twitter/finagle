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

package com.twitter.finagle.redis.naggati.test

import scala.collection.mutable
import com.twitter.finagle.redis.naggati._
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.channel._

class Counter {
  var readBytes = 0
  var writtenBytes = 0
}

object TestCodec {
  def apply[A: Manifest](firstStage: Stage, encoder: Encoder[A]) = {
    val counter = new Counter()
    val codec = new Codec(firstStage, encoder, { n => counter.readBytes += n },
      { n => counter.writtenBytes += n })
    val testCodec = new TestCodec(codec)
    (testCodec, counter)
  }
}

/**
 * Netty doesn't appear to have a good set of fake objects yet, so this wraps a Codec in a fake
 * environment that collects emitted objects and returns them.
 */
class TestCodec[A](val codec: Codec[A]) {
  val upstreamOutput = new mutable.ListBuffer[AnyRef]

  private def log(e: MessageEvent, list: mutable.ListBuffer[AnyRef]) {
    e.getMessage match {
      case buffer: ChannelBuffer =>
        val bytes = new Array[Byte](buffer.readableBytes)
        buffer.readBytes(bytes)
        list += bytes
      case x =>
        list += x
    }
  }

  private def toStrings(wrapped: Seq[Any]): Seq[String] = wrapped.map { item =>
    item match {
      case x: Array[Byte] => new String(x, "UTF-8")
      case x => x.toString
    }
  }

  val upstreamTerminus = new SimpleChannelUpstreamHandler() {
    override def messageReceived(c: ChannelHandlerContext, e: MessageEvent) {
      log(e, upstreamOutput)
    }
  }
  val pipeline = Channels.pipeline()
  pipeline.addLast("decoder", codec)
  pipeline.addLast("upstreamTerminus", upstreamTerminus)

  val context = pipeline.getContext(codec)
  val sink = new AbstractChannelSink() {
    def eventSunk(pipeline: ChannelPipeline, event: ChannelEvent) { }
  }
  val channel = new AbstractChannel(null, null, pipeline, sink) {
    def getRemoteAddress() = null
    def getLocalAddress() = null
    def isConnected() = true
    def isBound() = true
    def getConfig() = new DefaultChannelConfig()
  }

  def apply(buffer: ChannelBuffer) = {
    upstreamOutput.clear()
    codec.messageReceived(context, new UpstreamMessageEvent(pipeline.getChannel, buffer, null))
    upstreamOutput.toList
  }
}
