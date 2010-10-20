package com.twitter.finagle.thrift

import java.util.concurrent.atomic.AtomicReference
import java.lang.reflect.{Method, ParameterizedType, Proxy}
import scala.reflect.Manifest

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

class ServerThriftCodec[T](implementation: Object)(implicit man: Manifest[T]) extends ThriftCodec
{
  private def methodNamed(name: String, c: Class[_]): Option[Method] =
      c.getDeclaredMethods.find(_.getName == name)

  override def callForMessage(methodName: String): ThriftCall[_, _] = {
    val iface = man.erasure
    val ifaceMethod = methodNamed(methodName, iface)
    val implMethod = methodNamed(methodName, implementation.getClass)
    println("Interface method: %s".format(ifaceMethod))
    println("Implementation method: %s".format(implMethod))
    (ifaceMethod, implMethod) match {
      case (Some(iface: Method), Some(impl: Method)) =>
        // println("Iface: %s, Impl: %s".format(iface, impl))
        val implMethodParams = impl.getParameterTypes
        val implMethodExpectedParams = implMethodParams.slice(0, implMethodParams.size - 1)
        val ifaceParams = iface.getParameterTypes
        // val eqParams = (new HashSet(ifaceParams) ** HashSet(implMethodExpectedParams)).length == ifaceParams.length
        // println("Ret: Impl: %s, Iface: %s, Eq: %s".format(implMethodParams.last, iface.getReturnType, implMethodParams.last == iface.getReturnType))
        // val asyncDelegateType = implMethodParams.last
        // if (asyncDelegateType.isInstanceOf[ParameterizedType]) {
        //   val t = asyncDelegateType.asInstanceOf[ParameterizedType]
        //   println("Parameterized Type: %s".format(t.getActualTypeArguments()))
        // }

        // println("Sizes: Impl: %d, Iface: %d".format(implMethodExpectedParams.size, ifaceParams.size))
        // println("EqSize: %s".format())
        if (implMethodExpectedParams.size != ifaceParams.size)
          throw new IllegalArgumentException(
            "Arity mismatch, '%s' incongruent with '%s'".format(implMethodExpectedParams, ifaceParams))
        var compatible = true
        for (i <- 0 until ifaceParams.size) {
          println("%d: %s\t%s".format(i, ifaceParams(i), implMethodParams(i)))
          if (ifaceParams(i) ne implMethodParams(i))
            throw new IllegalArgumentException(
              "Expected '%s' to be of type '%s'".format(implMethodParams(i), ifaceParams(i)))
        }
        println("Will invoke %s".format(implMethod))
        // implMethod.invoke(implementation, List().toArray)
        // println("ImplMethodParams: %s".format(implMethodParams))
        // println("Expected: %s".format(implMethodExpectedParams))
//        println("EqP: %s, EqR: %s".format(eqParams, eqRet))
//        Proxy.getProxyClass(iface.getClass.getClassLoader, List(AsyncMethodCallback[ifaceMethod.getReturnType]).toArray)
//        val sig = iface.getParameterTypes + AsyncMethodCallback[ifaceMethod.getReturnType]
      case _ => () // GFY
    }

    null
  }
}

class ThriftCodec extends SimpleChannelHandler
{
  val protocolFactory = new TBinaryProtocol.Factory(true, true)
  val currentCall = new AtomicReference[ThriftCall[_, _ <: TBase[_]]]
  def callForMessage(method: String): ThriftCall[_, _] = throw new AbstractMethodError
  var reads = 0
  var writes = 0

  private def seqid = reads + writes

  override def handleDownstream(ctx: ChannelHandlerContext, c: ChannelEvent) {
    if (!c.isInstanceOf[MessageEvent]) {
      super.handleDownstream(ctx, c)
      return
    }

    val e = c.asInstanceOf[MessageEvent]
    writes += 1

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

        // Our reply is good! decode it & send it upstream.
        val result = currentCall.get().newResponseInstance
        // if (currentCall eq null) {
        //   // Find the method and its related args
        //   callForMessage(msg.name)
        // }

        var result = currentCall.newResponseInstance

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
