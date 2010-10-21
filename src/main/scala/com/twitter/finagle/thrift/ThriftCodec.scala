package com.twitter.finagle.thrift

import java.util.concurrent.atomic.AtomicReference
import java.lang.reflect.{Method, ParameterizedType, Proxy}
import scala.reflect.Manifest

import org.apache.thrift.{TBase, TApplicationException}
import org.apache.thrift.protocol.{TBinaryProtocol, TMessage, TMessageType, TProtocol}

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
(method: String, args: T)
(implicit val tman: Manifest[T], implicit val rman: Manifest[R]) {
  def newResponseInstance: R = rman.erasure.newInstance.asInstanceOf[R]
  def newArgInstance: T = tman.erasure.newInstance.asInstanceOf[T]
  def newInstance: ThriftCall[T,R] = new ThriftCall[T,R](method, newArgInstance)
}

case class ThriftReply[R <: TBase[_]]
(response: R, call: ThriftCall[_ <: TBase[_],_ <: TBase[_]])

object ThriftTypes extends scala.collection.mutable.HashMap[String, ThriftCall[_,_]] {
  def add(c: ThriftCall[_,_]): Unit = put(c.method, c)
}

class ThriftServerCodec extends ThriftCodec {
  override protected def server = true
}

class ThriftCodec extends SimpleChannelHandler {
  val protocolFactory = new TBinaryProtocol.Factory(true, true)
  val currentCall = new AtomicReference[ThriftCall[_, _ <: TBase[_]]]
  var seqid = if (server) 1 else 0
  protected def server = false

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
          val exc = new Exception("There may be only one outstanding Thrift call at a time")
          Channels.fireExceptionCaught(ctx, exc)
          c.getFuture.setFailure(exc)
          return
        }

        val writeBuffer = ChannelBuffers.dynamicBuffer()
        val oprot = protocolFactory.getProtocol(writeBuffer)

        seqid += 1
        oprot.writeMessageBegin(new TMessage(method, TMessageType.CALL, seqid))
        args.write(oprot)
        oprot.writeMessageEnd()
        Channels.write(ctx, c.getFuture, writeBuffer, e.getRemoteAddress)

      case thisReply@ThriftReply(response, call) =>
        val writeBuffer = ChannelBuffers.dynamicBuffer()
        val oprot = protocolFactory.getProtocol(writeBuffer)

        oprot.writeMessageBegin(new TMessage(call.method, TMessageType.REPLY, seqid))
        response.write(oprot)
        oprot.writeMessageEnd()
        Channels.write(ctx, c.getFuture, writeBuffer, e.getRemoteAddress)

      case _ =>
        val exc = new IllegalArgumentException("Unrecognized request type")
        Channels.fireExceptionCaught(ctx, exc)
        c.getFuture.setFailure(exc)
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
            "out of sequence response (got %d expected %d)".format(msg.seqid, seqid))
          Channels.fireExceptionCaught(ctx, exc)
          Channels.close(ctx, Channels.future(ctx.getChannel))
          return
        }

        if (server) {
          val request = ThriftTypes(msg.name).newInstance
          val args = request.args.asInstanceOf[TBase[_]]

          args.read(iprot)
          iprot.readMessageEnd()

          Channels.fireMessageReceived(ctx, request, e.getRemoteAddress)
        } else {
          val result = currentCall.get().newResponseInstance
          result.read(iprot)
          iprot.readMessageEnd()

          // Done with the current call cycle: we can now accept another
          // request.
          currentCall.set(null)

          Channels.fireMessageReceived(ctx, result, e.getRemoteAddress)
        }

      case _ =>
        Channels.fireExceptionCaught(
          ctx, new IllegalArgumentException("Unrecognized response type"))
    }
  }
}
