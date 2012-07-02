package com.twitter.finagle.protobuf.rpc
import com.google.protobuf.Message

/**
 * If the invocation of a service causes a RuntimeException this handler serializes it into a response message
 * that can be deserialized by an ExceptioResponseHandler.
 */
trait ServiceExceptionHandler[T] {

  def canHandle(e: RuntimeException): Boolean

  def handle(e: RuntimeException, m: Message): T

}