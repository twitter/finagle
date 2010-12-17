package com.twitter.finagle.thrift

import java.util.NoSuchElementException
import java.util.concurrent.atomic.AtomicReference
import java.lang.reflect.{Method, ParameterizedType, Proxy}

import scala.reflect.BeanProperty

import org.apache.thrift.{TBase, TApplicationException}
import org.apache.thrift.protocol.{TBinaryProtocol, TMessage, TMessageType, TProtocol}

import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}
import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.oneone.OneToOneDecoder
import org.jboss.netty.handler.codec.replay.{ReplayingDecoder, VoidEnum}

/*
 * Translate ThriftReplys to wire representation
 */
class ThriftServerEncoder extends SimpleChannelDownstreamHandler {
  protected val protocolFactory = new TBinaryProtocol.Factory(true, true)

  override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent) =
    e.getMessage match {
      case reply@ThriftReply(response, call) =>
        val buffer = ChannelBuffers.dynamicBuffer()
        val transport = new ChannelBufferTransport(buffer)
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
class ThriftServerDecoder extends ReplayingDecoder[VoidEnum] {
  protected val protocolFactory = new TBinaryProtocol.Factory(true, true)

  def decodeThriftCall(ctx: ChannelHandlerContext, channel: Channel,
                       buffer: ChannelBuffer):Object = {
    val transport = new ChannelBufferTransport(buffer)
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
            // XXX: Debug logging
            println("Casting in decodeThriftCall: %s".format(e))
            e.printStackTrace()
            null
        }
      case _ =>
        println("Non-call received in decodeThriftCall")
        // // Message types other than CALL are invalid here.
        // println("2222222222222222")
        // // We can't fire this exception since we're in a ReplayingDecoder
        // Channels.fireExceptionCaught(ctx,
        //   new TApplicationException(TApplicationException.INVALID_MESSAGE_TYPE))
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


/**
 * Translate ThriftCalls to their wire representation
 */
class ThriftClientEncoder extends SimpleChannelDownstreamHandler {
  protected val protocolFactory = new TBinaryProtocol.Factory(true, true)
  protected var seqid = 0

  override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent) =
    e.getMessage match {
      case call: ThriftCall[_, _] =>
        val buffer = ChannelBuffers.dynamicBuffer()
        val transport = new ChannelBufferTransport(buffer)
        val protocol = protocolFactory.getProtocol(transport)
        seqid += 1
        call.seqid = seqid
        call.writeRequest(seqid, protocol)
        Channels.write(ctx, Channels.succeededFuture(e.getChannel()),
                       buffer, e.getRemoteAddress)
      case _ =>
        Channels.fireExceptionCaught(ctx, new IllegalArgumentException)
        null
    }
}


/**
 * Translate wire representation to ThriftReply
 */
class ThriftClientDecoder extends ReplayingDecoder[VoidEnum] {
  protected val protocolFactory = new TBinaryProtocol.Factory(true, true)

  def decodeThriftReply(ctx: ChannelHandlerContext,
                        channel: Channel,
                        buffer: ChannelBuffer): Object = {
    val transport = new ChannelBufferTransport(buffer)
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
        val reply = call.readResponse(protocol)
        reply.asInstanceOf[AnyRef] // Note reply may not be a success
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
    // Thrift incorrectly assumes a read of zero bytes is an error, so treat
    // empty buffers as no-ops. This only happens with the ReplayingDecoder.
    if (buffer.readable)
      decodeThriftReply(ctx, channel, buffer)
    else
      null
}
