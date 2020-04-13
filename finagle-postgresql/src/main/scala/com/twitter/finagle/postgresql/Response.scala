package com.twitter.finagle.postgresql

sealed trait Response
object Response {

  private[postgresql] case class HandshakeResult(parameters: List[BackendMessage.ParameterStatus], backendData: BackendMessage.BackendKeyData) extends Response

  // TODO: remove this
  case class BackendResponse(e: BackendMessage) extends Response
}
