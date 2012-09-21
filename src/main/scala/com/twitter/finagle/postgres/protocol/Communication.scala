package com.twitter.finagle.postgres.protocol

case class PgRequest(msg: FrontendMessage) {

}

trait PgResponse {
  def asSingleMessage(): BackendMessage
}

case class SingleMessageResponse(msg: BackendMessage) extends PgResponse {
  override def asSingleMessage = msg
}

case class MessageSequenceResponse(messages: List[BackendMessage]) extends PgResponse {
  override def asSingleMessage = {
    throw new IllegalStateException("Sequence of messages returned")
  }
}

object Communication {

  def request(msg: FrontendMessage) = new PgRequest(msg)

  def response(msg: BackendMessage) = new SingleMessageResponse(msg)

  def sequence(messages: List[BackendMessage]) = new MessageSequenceResponse(messages)

}