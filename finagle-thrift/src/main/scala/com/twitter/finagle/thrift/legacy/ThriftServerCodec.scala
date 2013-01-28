package com.twitter.finagle.thrift

import org.apache.thrift.TApplicationException
import org.apache.thrift.protocol.{TMessageType, TProtocolFactory}

import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.replay.{ReplayingDecoder, VoidEnum}

import java.util.logging.{Logger, Level}

/*
 * Translate ThriftReplys to wire representation
 */
private[thrift] class ThriftServerEncoder(protocolFactory: TProtocolFactory)
    extends SimpleChannelDownstreamHandler
{
  override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent) =
    e.getMessage match {
      case reply@ThriftReply(response, call) =>
        val buffer = ChannelBuffers.dynamicBuffer()
        val transport = new ChannelBufferToTransport(buffer)
        val protocol = protocolFactory.getProtocol(transport)
        call.writeReply(call.seqid, protocol, response)
        Channels.write(ctx, Channels.succeededFuture(e.getChannel()), buffer, e.getRemoteAddress)
      case _ =>
        Channels.fireExceptionCaught(ctx, new IllegalArgumentException)
    }
}

/**
 * Translate wire representation to ThriftCalls
 */
private[thrift] class ThriftServerDecoder(protocolFactory: TProtocolFactory)
    extends ReplayingDecoder[VoidEnum]
{
  private[this] val logger = Logger.getLogger(getClass.getName)

  def decodeThriftCall(ctx: ChannelHandlerContext, channel: Channel,
                       buffer: ChannelBuffer):Object = {
    val transport = new ChannelBufferToTransport(buffer)
    val protocol = protocolFactory.getProtocol(transport)

    val message = protocol.readMessageBegin()

    message.`type` match {
      case TMessageType.CALL =>
        try {
          val factory = ThriftTypes(message.name)
          val request = factory.newInstance(message.seqid)
          request.readRequestArgs(protocol)
          request.asInstanceOf[AnyRef]
        } catch {
          // Pass through invalid message exceptions, etc.
          case e: TApplicationException =>
            logger.log(Level.FINE, e.getMessage, e)
            null
        }
      case _ =>
        // We can't respond with an error because we're in a replaying codec.
        null
    }
  }

  override def decode(ctx: ChannelHandlerContext,
                      channel: Channel,
                      buffer: ChannelBuffer,
                      state: VoidEnum) =
    // Thrift incorrectly assumes a read of zero bytes is an error, so treat
    // empty buffers as no-ops.
    if (buffer.readable)
      decodeThriftCall(ctx, channel, buffer)
    else
      null
}
