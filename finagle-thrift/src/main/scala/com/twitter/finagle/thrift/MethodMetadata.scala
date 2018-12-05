package com.twitter.finagle.thrift

import com.twitter.finagle.context.Contexts
import com.twitter.finagle.thrift.MethodMetadata.Key
import com.twitter.scrooge.ThriftMethod

/**
 * Stores information about the current Thrift method. This information is set
 * in both Java and Scala generated code. It is expected that users will only
 * need to interact with [[com.twitter.finagle.thrift.MethodMetadata.current]]
 * to obtain the current [[MethodMetadata]] if it has been set.
 *
 * @see [[com.twitter.finagle.thrift.MethodMetadata.current]]
 */
class MethodMetadata private[thrift] (
  val methodName: String,
  val serviceName: String,
  val argsClass: Class[_],
  val resultClass: Class[_]) {

  /**
   * Executes the given function with this [[MethodMetadata]] set as the current
   * [[MethodMetadata]]. The current [[MethodMetadata]] before executing this will be restored
   * on completion.
   */
  def asCurrent[T](f: => T): T = Contexts.local.let(Key, this)(f)

  override def toString: String =
    s"MethodMetadata(methodName=$methodName, serviceName=$serviceName, " +
      s"argsClass=${argsClass.getName}, resultClass=${resultClass.getName})"
}

/**
 * A [[com.twitter.finagle.context.LocalContext]] Key for a stored
 * [[com.twitter.finagle.thrift.MethodMetadata]] along with an accessor method
 * for retrieving the currently set value.
 */
object MethodMetadata {

  def apply(methodName: String, serviceName: String, argsClass: Class[_], resultClass: Class[_]) =
    new MethodMetadata(methodName, serviceName, argsClass, resultClass)

  def apply(thriftMethod: ThriftMethod): MethodMetadata = new MethodMetadata(
    methodName = thriftMethod.name,
    serviceName = thriftMethod.serviceName,
    argsClass = thriftMethod.argsCodec.metaData.structClass,
    resultClass = thriftMethod.responseCodec.metaData.structClass
  )

  /**
   * The [[com.twitter.finagle.context.LocalContext]] Key for the currently
   * stored [[com.twitter.finagle.thrift.MethodMetadata]]
   */
  val Key: Contexts.local.Key[MethodMetadata] =
    new Contexts.local.Key[MethodMetadata]

  /**
   * Returns the current [[MethodMetadata]] in context if it has been set.
   * @return `Some(MethodMetadata)` if the data has been set, otherwise, `None`.
   */
  def current: Option[MethodMetadata] =
    Contexts.local.get(Key)
}
