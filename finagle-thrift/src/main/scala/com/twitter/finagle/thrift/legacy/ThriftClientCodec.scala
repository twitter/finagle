package com.twitter.finagle.thrift

import org.apache.thrift.TApplicationException
import org.apache.thrift.protocol.{TMessageType, TProtocolFactory}

import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.replay.{ReplayingDecoder, VoidEnum}

/**
 * Translate ThriftCalls to their wire representation
 */
private[thrift] class ThriftClientEncoder(protocolFactory: TProtocolFactory)
    extends SimpleChannelDownstreamHandler
{
  protected var seqid = 0

  override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent) =
    e.getMessage match {
      case call: ThriftCall[_, _] =>
        val buffer = ChannelBuffers.dynamicBuffer()
        val transport = new ChannelBufferToTransport(buffer)
        val protocol = protocolFactory.getProtocol(transport)
        seqid += 1
        call.seqid = seqid
        call.writeRequest(seqid, protocol)
        Channels.write(ctx, Channels.succeededFuture(e.getChannel()),
                       buffer, e.getRemoteAddress)
      case _: Throwable =>
        Channels.fireExceptionCaught(ctx, new IllegalArgumentException)
    }
}

/**
 * Translate wire representation to ThriftReply
 */
private[thrift] class ThriftClientDecoder(protocolFactory: TProtocolFactory)
    extends ReplayingDecoder[VoidEnum]
{
  def decodeThriftReply(ctx: ChannelHandlerContext,
                        channel: Channel,
                        buffer: ChannelBuffer): Object = {
    val transport = new ChannelBufferToTransport(buffer)
    val protocol = protocolFactory.getProtocol(transport)
    val message = protocol.readMessageBegin()

    message.`type` match {
      case TMessageType.EXCEPTION =>
        // Create an event for any TApplicationExceptions.  Though these are
        // usually protocol-level errors, so there's not much the client can do.
        val exception = TApplicationException.read(protocol)
        protocol.readMessageEnd()
        Channels.fireExceptionCaught(ctx, exception)
        null
      case TMessageType.REPLY =>
        val call = ThriftTypes(message.name).newInstance()
        val result = call.readResponse(protocol).asInstanceOf[AnyRef]
        call.reply(result)
      case _ =>
        Channels.fireExceptionCaught(ctx, new TApplicationException(
          TApplicationException.INVALID_MESSAGE_TYPE))
        null
    }
  }

  override def decode(ctx: ChannelHandlerContext,
                      channel: Channel,
                      buffer: ChannelBuffer,
                      state: VoidEnum) =
    // TProtocol assumes a read of zero bytes is an error, so treat empty buffers
    // as no-ops. This only happens with the ReplayingDecoder.
    if (buffer.readable)
      decodeThriftReply(ctx, channel, buffer)
    else
      null
}
