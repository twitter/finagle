package com.twitter.finagle.thrift.transport.netty3

import com.twitter.finagle.thrift.ThriftClientRequest
import com.twitter.finagle.thrift.transport.ExceptionFactory
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.channel.{
  ChannelHandlerContext, Channels, MessageEvent, SimpleChannelDownstreamHandler}

/**
 * ThriftClientChannelBufferEncoder translates ThriftClientRequests to
 * bytes on the wire.
 */
private[netty3] class ThriftClientChannelBufferEncoder
  extends SimpleChannelDownstreamHandler
{
  override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent): Unit = {
    e.getMessage match {
      case request: ThriftClientRequest =>
        Channels.write(ctx, e.getFuture, ChannelBuffers.wrappedBuffer(request.message))

      case other =>
        val ex = ExceptionFactory.wrongClientWriteType(other)
        e.getFuture.setFailure(ex)
        throw ex
    }
  }
}
