package com.twitter.finagle.postgresql

import com.twitter.finagle.postgresql.BackendMessage.DataRow
import com.twitter.finagle.postgresql.BackendMessage.RowDescription

sealed trait Response
object Response {

  private[postgresql] case class HandshakeResult(parameters: List[BackendMessage.ParameterStatus], backendData: BackendMessage.BackendKeyData) extends Response

  // TODO: remove this
  case class BackendResponse(e: BackendMessage) extends Response

  // TODO: make this useful and streamable.
  case class ResultSet(rowDescription: RowDescription, rows: List[DataRow]) extends Response
}
