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

import scala.annotation.tailrec
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.frame.FrameDecoder

/*
 * Convenience exception class to allow decoders to indicate a protocol error.
 */
class ProtocolError(message: String, cause: Throwable) extends Exception(message, cause) {
  def this(message: String) = this(message, null)
}

/**
 * An Encoder turns things of type `A` into `ChannelBuffer`s, for outbound traffic (server
 * responses or client requests).
 */
trait Encoder[A] {
  /**
   * Convert an object of type `A` into a `ChannelBuffer`. If no buffer is returned, nothing is
   * written out.
   */
  def encode(obj: A): Option[ChannelBuffer]
}

object Codec {
  val NONE = new Encoder[Unit] {
    def encode(obj: Unit) = None
  }

  sealed abstract class Flag
  case object Disconnect extends Flag

  /**
   * Mixin for outbound (write-side) codec objects to allow them to be used for signalling
   * out-of-bound messages to the codec engine.
   *
   * Primarily this is used to signal that the connection should be closed after writing the
   * object. For example, if `Response` is a case class for writing a response, and `Signalling`
   * is mixed in, you can use:
   *
   *     channel.write(new Response(...) then Codec.Disconnect)
   *
   * to signal that the connection should be closed after writing the response.
   */
  trait Signalling {
    private var flags: List[Flag] = Nil

    /**
     * Add a signal flag to this outbound message.
     */
    def then(flag: Flag): this.type = {
      if (getClass.getName.endsWith("$")) {
        throw new Exception("Singletons can't be used for streaming.")
      }
      flags = flag :: flags
      this
    }

    def signals = flags

    override def toString = {
      super.toString + flags.map { _.toString }.mkString(" with Signalling(", ", ", ")")
    }
  }
}

object DontCareCounter extends (Int => Unit) {
  def apply(x: Int) { }
}

/**
 * A netty ChannelHandler for decoding data into protocol objects on the way in, and packing
 * objects into byte arrays on the way out. Optionally, the bytes in/out are tracked.
 */
class Codec[A: Manifest](
  firstStage: Stage,
  encoder: Encoder[A],
  bytesReadCounter: Int => Unit,
  bytesWrittenCounter: Int => Unit
) extends FrameDecoder with ChannelDownstreamHandler {
  def this(firstStage: Stage, encoder: Encoder[A]) =
    this(firstStage, encoder, DontCareCounter, DontCareCounter)

  private[this] var stage = firstStage

  private[this] def buffer(context: ChannelHandlerContext) = {
    ChannelBuffers.dynamicBuffer(context.getChannel.getConfig.getBufferFactory)
  }

  private[this] def encode(obj: A): Option[ChannelBuffer] = {
    val buffer = encoder.encode(obj)
    buffer.foreach { b => bytesWrittenCounter(b.readableBytes) }
    buffer
  }

  // turn an Encodable message into a Buffer.
  override final def handleDownstream(context: ChannelHandlerContext, event: ChannelEvent) {
    event match {
      case message: DownstreamMessageEvent => {
        val obj = message.getMessage
        if (manifest[A].runtimeClass.isAssignableFrom(obj.getClass)) {
          encode(obj.asInstanceOf[A]) match {
            case Some(buffer) =>
              Channels.write(context, message.getFuture, buffer, message.getRemoteAddress)
            case None =>
              message.getFuture.setSuccess()
          }
        } else {
          context.sendDownstream(event)
        }
        obj match {
          case signalling: Codec.Signalling => {
            signalling.signals.foreach {
              case Codec.Disconnect => context.getChannel.close()
              case _ =>
            }
          }
          case _ =>
        }
      }
      case _ => context.sendDownstream(event)
    }
  }

  @tailrec
  override final def decode(context: ChannelHandlerContext, channel: Channel, buffer: ChannelBuffer) = {
    val readableBytes = buffer.readableBytes()
    val nextStep = try {
      stage(buffer)
    } catch {
      case e: Throwable =>
        // reset state before throwing.
        stage = firstStage
        throw e
    }
    bytesReadCounter(readableBytes - buffer.readableBytes())
    nextStep match {
      case Incomplete =>
        null
      case GoToStage(s) =>
        stage = s
        decode(context, channel, buffer)
      case Emit(obj) =>
        stage = firstStage
        obj
    }
  }

  def pipelineFactory = new ChannelPipelineFactory() {
    def getPipeline = Channels.pipeline(Codec.this)
  }
}
