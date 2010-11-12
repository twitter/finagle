package com.twitter.finagle.thrift

import java.util.concurrent.atomic.AtomicReference
import java.lang.reflect.{Method, ParameterizedType, Proxy}

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

class ThriftCallFactory[A <: TBase[_], R <: TBase[_]](
  val method: String,
  argClass: Class[A],
  replyClass: Class[R])
{
  private[this] def newArgInstance() = argClass.newInstance
  def newInstance() = new ThriftCall(method, newArgInstance(), replyClass)
}

class ThriftCall[A <: TBase[_], R <: TBase[_]](
  method: String,
  args: A,
  replyClass: Class[R])
{
  private[thrift] def readRequestArgs(p: TProtocol) {
    args.read(p)
    p.readMessageEnd()
  }

  private[thrift] def writeRequest(seqid: Int, p: TProtocol) {
    p.writeMessageBegin(new TMessage(method, TMessageType.CALL, seqid))
    args.write(p)
    p.writeMessageEnd()
  }

  private[thrift] def writeReply(seqid: Int, p: TProtocol, reply: TBase[_]) {
    // Write server replies
    p.writeMessageBegin(new TMessage(method, TMessageType.REPLY, seqid))
    reply.write(p)
    p.writeMessageEnd()
  }

  private[thrift] def readResponse(p: TProtocol) = {
    // Read client responses
    val result = replyClass.newInstance()
    result.read(p)
    p.readMessageEnd()
    result
  }

  def newReply() = replyClass.newInstance()

  def reply(reply: R) =
    new ThriftReply[R](reply, this)

  def arguments: A = args.asInstanceOf[A]
}

case class ThriftReply[R <: TBase[_]]
(response: R, call: ThriftCall[_ <: TBase[_], _ <: TBase[_]])

object ThriftTypes extends scala.collection.mutable.HashMap[String, ThriftCallFactory[_, _]] {
  def add(c: ThriftCallFactory[_, _]): Unit = put(c.method, c)
  override def apply(method: String) = {
    try {
      super.apply(method)
    } catch {
      case e: java.util.NoSuchElementException =>
        throw new TApplicationException(
          TApplicationException.UNKNOWN_METHOD,
          "unknown method '%s'".format(method))
    }
  }
}

class ThriftServerCodec extends ThriftCodec
class ThriftClientCodec extends ThriftCodec


abstract class ThriftCodec extends SimpleChannelHandler {
  val protocolFactory = new TBinaryProtocol.Factory(true, true)
  val currentCall = new AtomicReference[ThriftCall[_, _]]
  var seqid = 0

  override def handleDownstream(ctx: ChannelHandlerContext, c: ChannelEvent) {
    if (!c.isInstanceOf[MessageEvent]) {
      super.handleDownstream(ctx, c)
      return
    }

    val e = c.asInstanceOf[MessageEvent]

    e.getMessage match {
      case thisCall: ThriftCall[_, _] =>
        // Dispatching requests as a client
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
        thisCall.writeRequest(seqid, oprot)
        Channels.write(ctx, c.getFuture, writeBuffer, e.getRemoteAddress)

      case thisReply@ThriftReply(response, call) =>
        // Writing replies as a server
        val writeBuffer = ChannelBuffers.dynamicBuffer()
        val oprot = protocolFactory.getProtocol(writeBuffer)

        call.writeReply(seqid, oprot, thisReply.response)
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

        if (isInstanceOf[ThriftServerCodec]) seqid += 1

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

        if (isInstanceOf[ThriftServerCodec]) {
          // Receiving requests as a server
          var request = try {
            ThriftTypes(msg.name).newInstance()
          } catch {
            case exc: Throwable =>
              Channels.fireExceptionCaught(ctx, exc)
              Channels.close(ctx, Channels.future(ctx.getChannel))
              return
          }
          request.readRequestArgs(iprot)

          Channels.fireMessageReceived(ctx, request, e.getRemoteAddress)
        } else {
          // Receiving replies as a client
          val result = currentCall.get().readResponse(iprot)

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
