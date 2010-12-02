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

/**
 * The ThriftCall object represents a thrift dispatch on the
 * channel. The method name & argument thrift structure (POJO) is
 * given.
 */
class ThriftCall[A <: TBase[_, _], R <: TBase[_, _]](
  @BeanProperty val method: String,
  args: A,
  replyClass: Class[R],
  val seqid: Int)
{
  // Constructor without seqno for Java
  def this(@BeanProperty method: String, args: A, replyClass: Class[R]) =
    this(method, args, replyClass, -1)

  private[thrift] def readRequestArgs(p: TProtocol) {
    args.read(p)
    p.readMessageEnd()
  }

  private[thrift] def writeRequest(seqid: Int, p: TProtocol) {
    p.writeMessageBegin(new TMessage(method, TMessageType.CALL, seqid))
    args.write(p)
    p.writeMessageEnd()
  }

  private[thrift] def writeReply(seqid: Int, p: TProtocol, reply: TBase[_, _]) {
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

  /**
   * Produce a new reply instance.
   */
  def newReply() = replyClass.newInstance()

  /**
   * Wrap a ReplyClass in a ThriftReply.
   */
  def reply(reply: R) =
    new ThriftReply[R](reply, this)

  /**
   * Read the argument list
   */
  def arguments: A = args.asInstanceOf[A]
}

/**
 * Encapsulates the result of a call to a Thrift service.
 */
case class ThriftReply[R <: TBase[_, _]](
  response: R,
  call: ThriftCall[_ <: TBase[_, _], _ <: TBase[_, _]])

class ThriftCallFactory[A <: TBase[_, _], R <: TBase[_, _]](
  val method: String,
  argClass: Class[A],
  replyClass: Class[R])
{
  private[this] def newArgInstance() = argClass.newInstance

  def newInstance(seqid: Int = -1):ThriftCall[A, R] =
    new ThriftCall(method, newArgInstance(), replyClass, seqid)

  def newInstance():ThriftCall[A, R] =
    new ThriftCall(method, newArgInstance(), replyClass, -1)
}

/**
 * A registry for Thrift types. Register ThriftCallFactory instances encapsulating
 * the types to be decoded by the ThriftServerCodec with this singleton.
 */
object ThriftTypes
  extends scala.collection.mutable.HashMap[String, ThriftCallFactory[_, _]]
{
  def add(c: ThriftCallFactory[_, _]): Unit = put(c.method, c)

  override def apply(method: String) = {
    try {
      super.apply(method)
    } catch {
      case e: NoSuchElementException =>
        throw new TApplicationException(
          TApplicationException.UNKNOWN_METHOD,
          "unknown method '%s'".format(method))
    }
  }
}

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

trait ThriftServerDecoderHelper {
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
          case e: TApplicationException => e
        }
      case _ =>
        // Message types other than CALL are invalid here.
        new TApplicationException(TApplicationException.INVALID_MESSAGE_TYPE)
    }
  }
}

/**
 * Thrift framed server decoder assumes messages are framed, so should be used
 * with the ThriftFrameCodec.
 */
class ThriftFramedServerDecoder extends OneToOneDecoder with ThriftServerDecoderHelper {
  override def decode(ctx: ChannelHandlerContext, channel: Channel, m: Object):Object =
    m match {
      case buffer:ChannelBuffer => decodeThriftCall(ctx, channel, buffer)
      case _                    => m
    }
}

/**
 * Thrift unframed server decoder doesn't assume messages are framed.
 *
 * The implementation repeated attempts to parse data until it succeeds.
 * This is fine for small requests from well-behaved clients, but will
 * impose a performance penality for large requests or requests from
 * misbehaved clients (e.g., sending one byte at a time).
 */
class ThriftUnframedServerDecoder extends ReplayingDecoder[VoidEnum]
with ThriftServerDecoderHelper {
  override def decode(ctx: ChannelHandlerContext, channel: Channel, buffer: ChannelBuffer,
                      state: VoidEnum) =
    decodeThriftCall(ctx, channel, buffer)
}

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
        call.writeRequest(seqid, protocol)
        Channels.write(ctx, Channels.succeededFuture(e.getChannel()), buffer, e.getRemoteAddress)
      case _ =>
        Channels.fireExceptionCaught(ctx, new IllegalArgumentException)
    }
}

trait ThriftClientDecoderHelper {
  protected val protocolFactory = new TBinaryProtocol.Factory(true, true)

  def decodeThriftReply(ctx: ChannelHandlerContext, channel: Channel,
                          buffer: ChannelBuffer):Object = {
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
        val exception = new TApplicationException(TApplicationException.INVALID_MESSAGE_TYPE)
        Channels.fireExceptionCaught(ctx, exception)
        null
    }
  }
}

/**
 * Thrift framed client decoder assumes messages are framed, so should be used
 * with the ThriftFrameCodec.
 */
class ThriftFramedClientDecoder extends OneToOneDecoder with ThriftClientDecoderHelper {
  override def decode(ctx: ChannelHandlerContext, channel: Channel, m: Object):Object =
    m match {
      case buffer:ChannelBuffer => decodeThriftReply(ctx, channel, buffer)
      case _                    => m
    }
}


/**
 * Thrift unframed client decoder doesn't assume messages are framed.
 *
 * The implementation repeated attempts to parse data until it succeeds.
 * This is fine for small responses from well-behaved servers, but will
 * impose a performance penality for large responses or responses from
 * misbehaved servers (e.g., sending one byte at a time).
 */
class ThriftUnframedClientDecoder extends ReplayingDecoder[VoidEnum]
with ThriftClientDecoderHelper {
  override def decode(ctx: ChannelHandlerContext, channel: Channel, buffer: ChannelBuffer,
                      state: VoidEnum) =
    decodeThriftReply(ctx, channel, buffer)
}
