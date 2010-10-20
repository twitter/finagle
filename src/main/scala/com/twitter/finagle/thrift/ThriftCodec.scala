package com.twitter.finagle.thrift

import scala.reflect.Manifest

import java.util.concurrent.atomic.AtomicReference

import org.apache.thrift.{TBase, TApplicationException}
import org.apache.thrift.protocol.{TBinaryProtocol, TMessage, TMessageType}

import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}
import org.jboss.netty.channel.{
  SimpleChannelHandler, ChannelHandlerContext,
  MessageEvent, ChannelEvent, Channels}

import ChannelBufferConversions._

/**
 * The ThriftCall object represents a thrift dispatch on the
 * channel. The method name & argument thrift structure (POJO) is
 * given.
 */
case class ThriftCall[T <: TBase[_], R <: TBase[_]]
(method: String, args: T)(implicit man: Manifest[R])
{
  def newResponseInstance: R = man.erasure.newInstance.asInstanceOf[R]
}

class ThriftCodec extends SimpleChannelHandler
{
  val protocolFactory = new TBinaryProtocol.Factory(true, true)
  var seqid = 0
  val currentCall = new AtomicReference[ThriftCall[_, _ <: TBase[_]]]

  override def handleDownstream(ctx: ChannelHandlerContext, c: ChannelEvent) {
    if (!c.isInstanceOf[MessageEvent]) {
      super.handleDownstream(ctx, c)
      return
    }

    val e = c.asInstanceOf[MessageEvent]

    e.getMessage match {
      case thisCall@ThriftCall(method, args) =>
        if (!currentCall.compareAndSet(null, thisCall)) {
          // TODO: is this the right ("netty") way of propagating
          // individual failures?  do we also want to throw it up the
          // channel?
          c.getFuture.setFailure(new Exception(
            "There may be only one outstanding Thrift call at a time"))
          return
        }

        val writeBuffer = ChannelBuffers.dynamicBuffer()
        val oprot = protocolFactory.getProtocol(writeBuffer)

        seqid += 1

        oprot.writeMessageBegin(new TMessage(method, TMessageType.CALL, seqid))
        args.write(oprot)
        oprot.writeMessageEnd()
        Channels.write(ctx, c.getFuture, writeBuffer, e.getRemoteAddress)

      case _ =>
        val exc = new IllegalArgumentException("Unrecognized request type")
        c.getFuture.setFailure(exc)
        throw exc
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
        val iprot = protocolFactory.getProtocol(buffer)
        val msg = iprot.readMessageBegin()

        if (msg.`type` == TMessageType.EXCEPTION) {
          val exc = TApplicationException.read(iprot)
          iprot.readMessageEnd()
          Channels.fireExceptionCaught(ctx, exc)
          currentCall.set(null)
          return
        }

        if (msg.seqid != seqid) {
          // This means the channel is in an inconsistent state, so we
          // both fire the exception (upstream), and close the channel
          // (downstream).
          val exc = new TApplicationException(
            TApplicationException.BAD_SEQUENCE_ID,
            "out of sequence response")
          Channels.fireExceptionCaught(ctx, exc)
          Channels.close(ctx, Channels.future(ctx.getChannel))
          return
        }

        // Our reply is good! decode it & send it upstream.
        val result = currentCall.get().newResponseInstance

        result.read(iprot)
        iprot.readMessageEnd()

        // Done with the current call cycle: we can now accept another
        // request.
        currentCall.set(null)

        Channels.fireMessageReceived(ctx, result, e.getRemoteAddress)

      case _ =>
        Channels.fireExceptionCaught(
          ctx, new IllegalArgumentException("Unrecognized response type"))
    }
  }
}
