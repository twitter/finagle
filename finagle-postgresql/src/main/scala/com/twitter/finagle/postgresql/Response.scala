package com.twitter.finagle.postgresql

import java.nio.charset.Charset
import java.time.ZoneId

import com.twitter.finagle.postgresql.Types.FieldDescription
import com.twitter.finagle.postgresql.Types.Name
import com.twitter.finagle.postgresql.Types.WireValue
import com.twitter.io.Reader
import com.twitter.util.Future

sealed trait Response
object Response {

  // For the Sync Query
  case object Ready extends Response

  case class ParsedParameters(
    serverEncoding: Charset,
    clientEncoding: Charset,
    timeZone: ZoneId,
  )
  case class ConnectionParameters(
    parameters: List[BackendMessage.ParameterStatus],
    // Not implemented in CRDB: https://github.com/cockroachdb/cockroach/pull/13009
    backendData: Option[BackendMessage.BackendKeyData],
  ) extends Response {

    lazy val parameterMap: Map[BackendMessage.Parameter, String] =
      parameters.map(param => param.key -> param.value).toMap

    lazy val parsedParameters: ParsedParameters = {
      // make sure the backend uses integers to store date time values.
      // Ancient Postgres versions used double and made this a compilation option.
      // Since Postgres 10, this is "on" by default and cannot be changed.
      // We still check, since this would have dire consequences on timestamp values.
      require(parameterMap(BackendMessage.Parameter.IntegerDateTimes) == "on", "integer_datetimes must be on.")

      ParsedParameters(
        serverEncoding = Charset.forName(parameterMap(BackendMessage.Parameter.ServerEncoding)),
        clientEncoding = Charset.forName(parameterMap(BackendMessage.Parameter.ClientEncoding)),
        timeZone = ZoneId.of(parameterMap(BackendMessage.Parameter.TimeZone))
      )
    }
  }
  object ConnectionParameters {
    val empty: ConnectionParameters = ConnectionParameters(Nil, None)
  }

  sealed trait QueryResponse extends Response
  type Row = IndexedSeq[WireValue]
  case class ResultSet(fields: IndexedSeq[FieldDescription], rows: Reader[Row], parameters: ConnectionParameters)
      extends QueryResponse {
    def toSeq: Future[Seq[Row]] = Reader.toAsyncStream(rows).toSeq()
    def buffered: Future[ResultSet] = toSeq.map(rows => ResultSet(fields, Reader.fromSeq(rows), parameters))
  }
  object Result {
    // def because Reader is stateful
    def empty: ResultSet = ResultSet(IndexedSeq.empty, Reader.empty, ConnectionParameters.empty)
  }
  case object Empty extends QueryResponse
  case class Command(commandTag: String) extends QueryResponse

  case class SimpleQueryResponse(responses: Reader[QueryResponse]) extends Response {
    def next: Future[QueryResponse] =
      responses.read().map(_.getOrElse(sys.error("expected at least one response, got none")))
  }

  // Extended query
  case class Prepared private[postgresql] (name: Name, parameterTypes: IndexedSeq[Types.Oid])
  case class ParseComplete(statement: Prepared) extends Response

}
