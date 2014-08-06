package com.twitter.finagle.protobuf.rpc

/**
 * Inspects a response message and if it contains an exception code it throws the correspondent exception. See also ServiceExceptionHandler.
 */
trait ExceptionResponseHandler[T] {

  def canHandle(message: T): Boolean

  def handle(message: T): RuntimeException

}