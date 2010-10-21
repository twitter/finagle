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
(method: String, args: T)(implicit val tman: Manifest[T], implicit val rman: Manifest[R])
{
  def newResponseInstance: R = rman.erasure.newInstance.asInstanceOf[R]
  def newArgInstance: T = tman.erasure.newInstance.asInstanceOf[T]
  def newInstance: ThriftCall[T,R] = new ThriftCall[T,R](method, newArgInstance)
}

case class ThriftResponse[R <: TBase[_]](response: R, call: ThriftCall[_ <: TBase[_],_ <: TBase[_]])

object ThriftTypes extends scala.collection.mutable.HashMap[String, ThriftCall[_,_]] {
  def add(c: ThriftCall[_,_]): Unit = put(c.method, c)
}

class ThriftServerCodec
extends ThriftCodec
{
  override protected def server = true
}

class ThriftCodec extends SimpleChannelHandler
{
  val protocolFactory = new TBinaryProtocol.Factory(true, true)
  val currentCall = new AtomicReference[ThriftCall[_, _ <: TBase[_]]]
  var reads = 0
  var writes = 0

  // FIXME: this should probably be pulled out to the top level to make it easier to access and emphasize its global-ness.

  private def seqid = reads + writes
  protected def server = false

  override def handleDownstream(ctx: ChannelHandlerContext, c: ChannelEvent) {
    println("handleDownstream")
    if (!c.isInstanceOf[MessageEvent]) {
      super.handleDownstream(ctx, c)
      return
    }

    val e = c.asInstanceOf[MessageEvent]

    if (!server)
      writes += 1

    e.getMessage match {
      case thisRequest@ThriftCall(method, args) =>
        if (!currentCall.compareAndSet(null, thisRequest)) {
          // TODO: is this the right ("netty") way of propagating
          // individual failures?  do we also want to throw it up the
          // channel?
          c.getFuture.setFailure(new Exception(
            "There may be only one outstanding Thrift call at a time"))
          return
        }

        val writeBuffer = ChannelBuffers.dynamicBuffer()
        val oprot = protocolFactory.getProtocol(writeBuffer)

        oprot.writeMessageBegin(new TMessage(method, TMessageType.CALL, seqid))
        args.write(oprot)
        oprot.writeMessageEnd()
        Channels.write(ctx, c.getFuture, writeBuffer, e.getRemoteAddress)
      // Handle wrapped thrift response
      case thisResponse@ThriftResponse(response, call) =>
        val writeBuffer = ChannelBuffers.dynamicBuffer()
        val oprot = protocolFactory.getProtocol(writeBuffer)

        oprot.writeMessageBegin(new TMessage(call.method, TMessageType.REPLY, seqid))
        response.write(oprot)
        oprot.writeMessageEnd()
        println("Handled response in handleDownstream: Response=[%s] Call=[%s]".format(response, call))
        Channels.write(ctx, c.getFuture, writeBuffer, e.getRemoteAddress)
      case _ =>
        val exc = new IllegalArgumentException("Unrecognized request type")
        c.getFuture.setFailure(exc)
        throw exc
    }
  }

  override def handleUpstream(ctx: ChannelHandlerContext, c: ChannelEvent) {
    println("handleUpstream")
    if (!c.isInstanceOf[MessageEvent]) {
      super.handleUpstream(ctx, c)
      return
    }

    if (server)
      reads += 1

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

        if (false && msg.seqid != seqid) { // FIXME: this needs to be fixed. The sequence number needs to be checked.
          println("Sequence ID crap")
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

        println(getClass.getName)
        println("\tCurrentCall: %s".format(currentCall))

        if (server) {
          // Find the method and its related args
          val request = ThriftTypes(msg.name).newInstance
          val args = request.args.asInstanceOf[TBase[_]]
          args.read(iprot)
          iprot.readMessageEnd()
          println("Server: Received message: %s".format(request))
          Channels.fireMessageReceived(ctx, request, e.getRemoteAddress)
        } else {
          println("Client")

          // Our reply is good! decode it & send it upstream.
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
