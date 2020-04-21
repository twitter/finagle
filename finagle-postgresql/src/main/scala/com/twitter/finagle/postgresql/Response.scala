package com.twitter.finagle.postgresql

import com.twitter.finagle.postgresql.BackendMessage.DataRow
import com.twitter.finagle.postgresql.Types.FieldDescription
import com.twitter.finagle.postgresql.Types.Name
import com.twitter.io.Reader
import com.twitter.util.Future

sealed trait Response
object Response {

  private[postgresql] case class HandshakeResult(parameters: List[BackendMessage.ParameterStatus], backendData: BackendMessage.BackendKeyData) extends Response

  // TODO: remove this
  case class BackendResponse(e: BackendMessage) extends Response

  sealed trait QueryResponse extends Response
  // TODO: make this useful
  case class ResultSet(fields: IndexedSeq[FieldDescription], rows: Reader[DataRow]) extends QueryResponse {
    def toSeq: Future[Seq[DataRow]] = Reader.toAsyncStream(rows).toSeq()
  }
  case object Empty extends QueryResponse
  case class Command(commandTag: String) extends QueryResponse

  case class SimpleQueryResponse(responses: Reader[QueryResponse]) extends Response {
    def next: Future[QueryResponse] =
      responses.read().map(_.getOrElse(sys.error("expected at least one response, got none")))
  }

  // Extended query
  case class Prepared private[postgresql](name: Name, parameters: IndexedSeq[Types.Oid])
  case class ParseComplete(statement: Prepared) extends Response

}
