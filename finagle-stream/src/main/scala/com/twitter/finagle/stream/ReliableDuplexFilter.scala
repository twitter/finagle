package com.twitter.finagle.stream

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.channel._

import com.twitter.concurrent.{Channel, ChannelSource}
import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.finagle.util.Conversions._
import com.twitter.util.{Future, Promise, Return, Throw}

/**
 * ReliableDuplexFilter wraps messages sent down a channel buffer in order
 * to delay message acknowledgement until the other ends' ReliableDuplexFilter
 * acknowledges the send.
 *
 * This filter is added by DuplexStreamCodec when the first constructor argument
 * is `true`.
 */

abstract sealed class MessageType(val id: Byte) {
  MessageType.addType(this)
}
case object Push extends MessageType(0)
case object AckReturn extends MessageType(1)
case object AckThrow extends MessageType(2)

object MessageType {
  private[this] val types = new Array[MessageType](256)

  def apply(id: Byte) = {
    Option(types(id - Byte.MinValue))
  }

  private[stream] def addType(messageType: MessageType) {
    types(messageType.id - Byte.MinValue) = messageType
  }
}

abstract class ReliableDuplexFilter
  extends SimpleFilter[Channel[ChannelBuffer], Channel[ChannelBuffer]]
{

  protected type Cb = ChannelBuffer
  private[this] val nextMessageId = new AtomicInteger(0)
  private[this] val pendingAcks = new ConcurrentHashMap[Int, Promise[Unit]]()

  protected[this] def prepareOutbound(outbound: Channel[Cb], wrapped: ChannelSource[Cb]) = {
    outbound respond { buf =>
      val messageId = nextMessageId.getAndIncrement()
      val promise = new Promise[Unit]
      pendingAcks.put(messageId, promise)
      wrapped send encode(Push, messageId, buf)
      promise
    }

    wrapped
  }

  protected[this] def prepareInbound(inbound: Channel[Cb], outbound: ChannelSource[Cb]): Channel[Cb] = {
    val unwrapped = new ChannelSource[Cb]

    inbound respond { buf =>
      val (messageType, messageId, data) = decode(buf)

      // We ack `ch`'s send immediately, but we wait for unwrappedInbound to ack
      // before we send an explicit ack message
      messageType match {
        case Push =>
          Future.join(unwrapped send data) onSuccess { _ =>
            outbound send encode(AckReturn, messageId, ChannelBuffers.EMPTY_BUFFER)
          } onFailure { _ =>
            outbound send encode(AckThrow, messageId, ChannelBuffers.EMPTY_BUFFER)
          }
        case AckReturn =>
          pendingAcks.remove(messageId)() = Return(Unit)
        case AckThrow =>
          pendingAcks.remove(messageId)() = Throw(new Exception("Channel send failed"))
      }
      Future.Unit
    }

    unwrapped
  }

  private[this] def decode(buf: ChannelBuffer): (MessageType, Int, ChannelBuffer) = {
    val remainingLength = buf.readableBytes - 5

    val messageType = MessageType(buf.readByte)
    if (!messageType.isDefined) {
      throw new Exception("Unknown message type")
    }

    (messageType.get, buf.readInt, buf.slice(5, remainingLength))
  }

  private[this] def encode(messageType: MessageType, messageId: Int, buf: ChannelBuffer): ChannelBuffer = {
    val header = ChannelBuffers.buffer(5)
    header.writeByte(messageType.id)
    header.writeInt(messageId)

    ChannelBuffers.wrappedBuffer(header, buf)
  }

}

class ReliableDuplexServerFilter extends ReliableDuplexFilter {
  def apply(reqIn: Channel[Cb], service: Service[Channel[Cb], Channel[Cb]]): Future[Channel[Cb]] = {
    val repOut = new ChannelSource[Cb]
    val reqOut = prepareInbound(reqIn, repOut)

    service(reqOut) map { repIn =>
      prepareOutbound(repIn, repOut)
    }
  }
}

class ReliableDuplexClientFilter extends ReliableDuplexFilter {
  def apply(reqIn: Channel[Cb], service: Service[Channel[Cb], Channel[Cb]]): Future[Channel[Cb]] = {
    val reqOut = new ChannelSource[Cb]
    prepareOutbound(reqIn, reqOut)

    service(reqOut) map { repIn =>
      prepareInbound(repIn, reqOut)
    }
  }
}