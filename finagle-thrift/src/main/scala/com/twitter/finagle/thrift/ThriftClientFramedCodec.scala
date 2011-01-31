package com.twitter.finagle.thrift

import org.jboss.netty.channel.{
  SimpleChannelHandler, Channel, ChannelEvent, ChannelHandlerContext,
  SimpleChannelDownstreamHandler, MessageEvent, Channels,
  ChannelPipelineFactory}
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder

import com.twitter.finagle.Codec
import com.twitter.finagle.util.{Ok, Error, Cancelled}
import com.twitter.finagle.util.Conversions._
import com.twitter.finagle.CancelledRequestException

case class ThriftClientRequest(message: Array[Byte], oneway: Boolean)

class ThriftClientChannelBufferEncoder extends SimpleChannelDownstreamHandler {
  override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent) {
    e.getMessage match {
      case ThriftClientRequest(message, oneway) =>
        Channels.write(ctx, e.getFuture, ChannelBuffers.wrappedBuffer(message))
        if (oneway) {
          // oneway RPCs are satisfied when the write is complete.
          e.getFuture() {
            case Ok(_) =>
              Channels.fireMessageReceived(ctx, ChannelBuffers.EMPTY_BUFFER)
            case Error(e) =>
              Channels.fireExceptionCaught(ctx, e)
            case Cancelled =>
              Channels.fireExceptionCaught(ctx, new CancelledRequestException)
          }
        }

      case _ =>
        throw new IllegalArgumentException("no byte array")
    }
  }
}

object ThriftClientFramedCodec {
  def apply() = new ThriftClientFramedCodec
}

class ThriftClientFramedCodec extends Codec[ThriftClientRequest, Array[Byte]] {
  val clientPipelineFactory =
    new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = Channels.pipeline()
        pipeline.addLast("thriftFrameCodec", new ThriftFrameCodec)
        pipeline.addLast("byteEncoder", new ThriftClientChannelBufferEncoder)
        pipeline.addLast("byteDecoder", new ThriftChannelBufferDecoder)
        pipeline
      }
    }

  val serverPipelineFactory = clientPipelineFactory
}
