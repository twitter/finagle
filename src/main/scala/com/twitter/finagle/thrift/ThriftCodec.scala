package com.twitter.finagle.thrift

import scala.reflect.Manifest

import org.apache.thrift.{TBase, TApplicationException}
import org.apache.thrift.protocol.{TBinaryProtocol, TMessage, TMessageType}
import org.apache.thrift.transport.{
  AutoExpandingBufferWriteTransport, AutoExpandingBufferReadTransport}

import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}
import org.jboss.netty.channel.{
  SimpleChannelHandler, ChannelHandlerContext,
  MessageEvent, ChannelEvent, Channels}

case class ThriftCall[T <: TBase[_, _], R <: TBase[_, _]]
(method: String, args: T)(implicit man: Manifest[R])
{
  def newResponseInstance: R = man.erasure.newInstance.asInstanceOf[R]
}

class ThriftCodec extends SimpleChannelHandler
{
  val factory = new TBinaryProtocol.Factory(true, true)
  var seqid = 0
  @volatile var currentCall: ThriftCall[_, _ <: TBase[_, _]] = null

  override def handleDownstream(ctx: ChannelHandlerContext, c: ChannelEvent) {
    if (!c.isInstanceOf[MessageEvent]) {
      super.handleDownstream(ctx, c)
      return
    }

    val e = c.asInstanceOf[MessageEvent]

    e.getMessage match {
      case call@ThriftCall(method, args) =>
        currentCall = call
        val transport = new WritableChannelBufferTransport
        val oprot = factory.getProtocol(transport)

        seqid += 1

        oprot.writeMessageBegin(new TMessage(method, TMessageType.CALL, seqid))
        args.write(oprot)
        oprot.writeMessageEnd()
        Channels.write(ctx, c.getFuture, transport.writeableBuffer, e.getRemoteAddress)

      case _ =>
        throw new IllegalArgumentException("Unrecognized request type")
    }
  }

  override def handleUpstream(ctx: ChannelHandlerContext, c: ChannelEvent) {
    if (!c.isInstanceOf[MessageEvent]) {
      super.handleUpstream(ctx, c)
      return
    }

    val e = c.asInstanceOf[MessageEvent]

    e.getMessage match {
      case buffer: ChannelBuffer =>
        val transport = new ReadableChannelBufferTransport(buffer)
        val iprot = factory.getProtocol(transport)      
        val msg = iprot.readMessageBegin()

        if (msg.`type` == TMessageType.EXCEPTION) {
          val exc = TApplicationException.read(iprot)
          iprot.readMessageEnd()
          Channels.fireExceptionCaught(ctx, exc)
          return
        }

        if (msg.seqid != seqid) {
          // TODO: This means the channel is in an inconsistent
          // state. Should we request a close? What do other protocol
          // codecs do?
          val exc = new TApplicationException(
            TApplicationException.BAD_SEQUENCE_ID,
            "out of sequence response")
          Channels.fireExceptionCaught(ctx, exc)
          return
        }

        // We have a good reply: decode it & send it upstream.
        val result = currentCall.newResponseInstance

        result.read(iprot)
        iprot.readMessageEnd()

        Channels.fireMessageReceived(ctx, result, e.getRemoteAddress)

      case _ =>
        Channels.fireExceptionCaught(
          ctx, new IllegalArgumentException("Unrecognized response type"))
    }
  }
}
