package com.twitter.finagle.postgres

import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.builder.ClientBuilder
import protocol._
import com.twitter.util.Future
import java.util.concurrent.atomic.AtomicInteger
import protocol.Describe
import protocol.Field
import protocol.CommandCompleteResponse
import protocol.Parse
import protocol.PgRequest
import protocol.Query
import protocol.SelectResult
import protocol.RowDescriptions
import protocol.Rows
import protocol.StringValue
import protocol.Value
import org.jboss.netty.buffer.ChannelBuffer

class Client(factory: ServiceFactory[PgRequest, PgResponse]) {
  private[this] lazy val underlying = factory.apply()
  private[this] val counter = new AtomicInteger(0)

  def query(sql: String): Future[QueryResponse] = sendQuery(sql) {
    case SelectResult(fields, rows) => Future(ResultSet(fields, rows))
    case CommandCompleteResponse(affected) => Future(OK(affected))
  }

  def fetch(sql: String): Future[SelectResult] = sendQuery(sql) {
    case rs: SelectResult => Future(rs)
  }

  def executeUpdate(sql: String): Future[CommandCompleteResponse] = sendQuery(sql) {
    case ok: CommandCompleteResponse => Future(ok)
  }

  def select[T](sql: String)(f: Row => T): Future[Seq[T]] = fetch(sql) map {
    rs =>
      extractRows(rs).map(f)
  }

  def prepare(sql: String): Future[PreparedStatement] = for {
    name <- parse(sql)
  } yield new PreparedStatementImpl(name)

  def close() {
    factory.close()
  }

  private[this] def parse(sql: String): Future[String] = {
    val name = genName()
    send(PgRequest(Parse(name, sql), flush = true)) {
      case ParseCompletedResponse => Future.value(name)
    }
  }

  private[this] def bind(name: String): Future[Unit] = send(PgRequest(Bind(portal = name, name = name), flush = true)) {
    case BindCompletedResponse => Future.value(())
  }

  private[this] def describe(name: String): Future[(IndexedSeq[String], IndexedSeq[ChannelBuffer => Value])] = send(PgRequest(Describe(portal = true, name = name), flush = true)) {
    case RowDescriptions(fields) => Future.value(processFields(fields))
  }

  private[this] def execute(name: String, maxRows: Int = 0) = fire(PgRequest(Execute(name, maxRows), flush = true))

  private[this] def sync(): Future[Unit] = send(PgRequest(Sync)) {
    case ReadyForQueryResponse => Future.value(())
  }


  private[this] def sendQuery[T](sql: String)(handler: PartialFunction[PgResponse, Future[T]]) = send(PgRequest(new Query(sql)))(handler)

  private[this] def fire(r: PgRequest) = underlying flatMap {
    service => service(r)
  }

  private[this] def send[T](r: PgRequest)(handler: PartialFunction[PgResponse, Future[T]]) = fire(r) flatMap (handler orElse {
    case some => throw new UnsupportedOperationException("TODO Support exceptions correctly " + some)
  })

  private[this] def processFields(fields: IndexedSeq[Field]): (IndexedSeq[String], IndexedSeq[ChannelBuffer => Value]) = {
    val names = fields.map(f => f.name)
    val parsers = fields.map(f => ValueParser.parserOf(f.format, f.dataType))

    (names, parsers)
  }

  private[this] def extractRows(rs: SelectResult): List[Row] = {
    val (fieldNames, fieldParsers) = processFields(rs.fields)

    rs.rows.map(dataRow => new Row(fieldNames, dataRow.data.zip(fieldParsers).map(pair => pair._2(pair._1))))
  }

  private[this] class PreparedStatementImpl(name: String) extends PreparedStatement {
    def exec(params: AnyRef*): Future[QueryResponse] = {
      for {
        _ <- bind(name)
        (fieldNames, fieldParsers) <- describe(name)
        exec <- execute(name)
        _ <- sync()
      } yield exec match {
        case CommandCompleteResponse(rows) => OK(rows)
        case Rows(rows, true) => ResultSet(fieldNames, fieldParsers, rows)
      }
    }
  }


  private[this] def genName() = "fin-pg-" + counter.incrementAndGet

}

object Client {

  def apply(host: String, username: String, password: Option[String], database: String): Client = {
    val factory: ServiceFactory[PgRequest, PgResponse] = ClientBuilder()
      .codec(new PgCodec(username, password, database))
      .hosts(host)
      .hostConnectionLimit(1)
      .buildFactory()

    new Client(factory)
  }

}

class Row(val fields: IndexedSeq[String], val vals: IndexedSeq[Value]) {

  private[this] val indexMap = fields.zipWithIndex.toMap

  def get(name: String): Option[Value] = {
    indexMap.get(name).map(vals(_))
  }

  def getString(name: String): String = {
    val value = get(name) map {
      case StringValue(s) => s
      case _ => throw new IllegalStateException("Expected string value")
    }
    value.get
  }

  def get(index: Int): Value = vals(index)

  def values(): IndexedSeq[Value] = vals

  override def toString = "{ fields='" + fields.toString + "', rows='" + vals.toString + "'}"

}

sealed trait QueryResponse

case class OK(affectedRows: Int) extends QueryResponse

case class ResultSet(rows: List[Row]) extends QueryResponse

object ResultSet {

  def apply(fieldNames: IndexedSeq[String], fieldParsers: IndexedSeq[ChannelBuffer => Value], rows: List[DataRow]) = new ResultSet(rows.map(dataRow => new Row(fieldNames, dataRow.data.zip(fieldParsers).map(pair => pair._2(pair._1)))))

  def apply(fields: IndexedSeq[Field], rows: List[DataRow]): ResultSet = {
    val (fieldNames, fieldParsers) = processFields(fields)

    apply(fieldNames, fieldParsers, rows)
  }

  private[this] def processFields(fields: IndexedSeq[Field]): (IndexedSeq[String], IndexedSeq[ChannelBuffer => Value]) = {
    val names = fields.map(f => f.name)
    val parsers = fields.map(f => ValueParser.parserOf(f.format, f.dataType))

    (names, parsers)
  }

}

trait PreparedStatement {
  def exec(params: AnyRef*): Future[QueryResponse]
}
