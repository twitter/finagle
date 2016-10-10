package com.twitter.finagle.thrift.transport.netty3

import com.twitter.finagle.thrift.transport.ExceptionFactory
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.channel.{
  ChannelHandlerContext, Channels, MessageEvent, SimpleChannelDownstreamHandler}

private[netty3] class ThriftServerChannelBufferEncoder
  extends SimpleChannelDownstreamHandler
{
  override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent): Unit = {
    e.getMessage match {
      case array: Array[Byte] =>
        val buffer = ChannelBuffers.wrappedBuffer(array)
        Channels.write(ctx, e.getFuture, buffer)

      case other =>
        val ex = ExceptionFactory.wrongServerWriteType(other)
        e.getFuture.setFailure(ex)
        throw ex
    }
  }
}
