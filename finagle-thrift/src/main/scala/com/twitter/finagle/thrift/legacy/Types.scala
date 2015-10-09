package com.twitter.finagle.thrift

import org.apache.thrift.protocol.{TMessage, TMessageType, TProtocol}
import org.apache.thrift.{TBase, TApplicationException}
import scala.beans.BeanProperty
import scala.language.existentials

/**
 * The ThriftCall object represents a thrift dispatch on the
 * channel. The method name & argument thrift structure (POJO) is
 * given.
 */
class ThriftCall[A <: TBase[_, _], R <: TBase[_, _]](
  @BeanProperty val method: String,
  args: A,
  replyClass: Class[R],
  var seqid: Int)
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
  def reply(reply: AnyRef) =
    new ThriftReply[R](reply.asInstanceOf[R], this)

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
 *
 * Server and client codecs will use these types for marshalling.
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
