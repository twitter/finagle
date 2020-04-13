package com.twitter.finagle.postgresql

import com.twitter.finagle.postgresql.BackendMessage.DataRow
import com.twitter.finagle.postgresql.BackendMessage.RowDescription
import com.twitter.io.Reader
import com.twitter.util.Future

sealed trait Response
object Response {

  private[postgresql] case class HandshakeResult(parameters: List[BackendMessage.ParameterStatus], backendData: BackendMessage.BackendKeyData) extends Response

  // TODO: remove this
  case class BackendResponse(e: BackendMessage) extends Response

  // TODO: make this useful
  case class ResultSet(rowDescription: RowDescription, rows: Reader[DataRow]) extends Response {
    def toSeq: Future[Seq[DataRow]] = Reader.toAsyncStream(rows).toSeq()
  }
}
