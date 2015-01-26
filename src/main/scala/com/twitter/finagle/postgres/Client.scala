package com.twitter.finagle.postgres

import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.postgres.codec.{CustomOIDProxy, PgCodec, Errors}
import com.twitter.finagle.postgres.messages._
import com.twitter.finagle.postgres.values.{StringValueEncoder, ValueParser, Value}
import com.twitter.util.Future

import java.util.concurrent.atomic.AtomicInteger

import org.jboss.netty.buffer.ChannelBuffer

import scala.util.Random

/*
 * A Finagle client for communicating with Postgres.
 */
class Client(factory: ServiceFactory[PgRequest, PgResponse], id:String) {
  private[this] lazy val underlying = factory.apply()
  private[this] val counter = new AtomicInteger(0)
  private[this] lazy val customTypes = CustomOIDProxy.serviceOIDMap(id)

  def query(sql: String): Future[QueryResponse] = sendQuery(sql) {
    case SelectResult(fields, rows) => Future(ResultSet(fields, rows, customTypes))
    case CommandCompleteResponse(affected) => Future(OK(affected))
  }

  def fetch(sql: String): Future[SelectResult] = sendQuery(sql) {
    case rs: SelectResult => Future(rs)
  }

  def executeUpdate(sql: String): Future[OK] = sendQuery(sql) {
    case CommandCompleteResponse(rows) => Future(OK(rows))
  }

  def select[T](sql: String)(f: Row => T): Future[Seq[T]] = fetch(sql) map {
    rs =>
      extractRows(rs).map(f)
  }

  def selectFirst[T](sql: String)(f: Row => T): Future[Option[T]] =
    select[T](sql)(f) flatMap { rows => Future.value(rows.headOption)}

  def prepare(sql: String): Future[PreparedStatement] = for {
    name <- parse(sql)
  } yield new PreparedStatementImpl(name)

  def close() = {
    underlying flatMap { service =>
      resetConnection() flatMap { response => service.close()}
    }
  }

  private[this] def resetConnection(): Future[QueryResponse] = {
    sync() flatMap { _ => query("DISCARD ALL;")}
  }

  private[this] def parse(sql: String): Future[String] = {
    val name = genName()
    send(PgRequest(Parse(name, sql), flush = true)) {
      case ParseCompletedResponse => Future.value(name)
    }
  }

  private[this] def bind(name: String, params: Seq[ChannelBuffer] = Seq()): Future[Unit] =
    send(PgRequest(Bind(portal = name, name = name, params = params), flush = true)) {
      case BindCompletedResponse => Future.value(())
    }

  private[this] def describe(name: String): Future[(IndexedSeq[String], IndexedSeq[ChannelBuffer => Value[Any]])] = {
    send(PgRequest(Describe(portal = true, name = name), flush = true)) {
      case RowDescriptions(fields) => Future.value(processFields(fields))
    }
  }

  private[this] def execute(name: String, maxRows: Int = 0) = fire(PgRequest(Execute(name, maxRows), flush = true))

  private[this] def sync(): Future[Unit] = send(PgRequest(Sync)) {
    case ReadyForQueryResponse => Future.value(())
  }

  private[this] def sendQuery[T](sql: String)(handler: PartialFunction[PgResponse, Future[T]]) = {
    send(PgRequest(new Query(sql)))(handler)
  }

  private[this] def fire(r: PgRequest) = underlying flatMap {
    service => service(r)
  }

  private[this] def send[T](r: PgRequest)(handler: PartialFunction[PgResponse, Future[T]]) = {
    fire(r) flatMap (handler orElse {
      case some => throw new UnsupportedOperationException("TODO Support exceptions correctly " + some)
    })
  }

  private[this] def processFields(
      fields: IndexedSeq[Field]): (IndexedSeq[String], IndexedSeq[ChannelBuffer => Value[Any]]) = {
    val names = fields.map(f => f.name)
    val parsers = fields.map(f => ValueParser.parserOf(f.format, f.dataType, customTypes))

    (names, parsers)
  }

  private[this] def extractRows(rs: SelectResult): List[Row] = {
    val (fieldNames, fieldParsers) = processFields(rs.fields)

    rs.rows.map(dataRow => new Row(fieldNames, dataRow.data.zip(fieldParsers).map {
      case (d, p) => if (d == null) null else p(d)
    }))
  }

  private[this] class PreparedStatementImpl(name: String) extends PreparedStatement {
    def fire(params: Any*): Future[QueryResponse] = {
      val binaryParams = params.map(p => StringValueEncoder.encode(p))
      val f = for {
        _ <- bind(name, binaryParams)
        (fieldNames, fieldParsers) <- describe(name)
        exec <- execute(name)
      } yield exec match {
          case CommandCompleteResponse(rows) => OK(rows)
          case Rows(rows, true) => ResultSet(fieldNames, fieldParsers, rows, customTypes)
        }
      f transform {
        result =>
          sync().flatMap {
            _ => Future.const(result)
          }
      }
    }
  }

  private[this] def genName() = "fin-pg-" + counter.incrementAndGet
}

/*
 * Helper companion object that generates a client from authentication information.
 *
 * TODO: Add other options (e.g., number of connections to keep open).
 */
object Client {
  def apply(
      host: String,
      username: String,
      password: Option[String],
      database: String,
      useSsl: Boolean = false): Client = {
    val id = Random.alphanumeric.take(28).mkString

    val factory: ServiceFactory[PgRequest, PgResponse] = ClientBuilder()
      .codec(new PgCodec(username, password, database, id, useSsl = useSsl))
      .hosts(host)
      .hostConnectionLimit(1)
      .buildFactory()

    new Client(factory, id)
  }
}

/*
 * A query that supports parameter substitution. Can help prevent SQL injection attacks.
 */
trait PreparedStatement {
  def fire(params: Any*): Future[QueryResponse]

  def exec(params: Any*): Future[OK] = fire(params: _*) map {
    case ok: OK => ok
    case ResultSet(_) => throw Errors.client("Update query expected")
  }

  def select[T](params: Any*)(f: Row => T): Future[Seq[T]] = fire(params: _*) map {
    case ResultSet(rows) => rows.map(f)
    case OK(_) => throw Errors.client("Select query expected")
  }

  def selectFirst[T](params: Any*)(f: Row => T): Future[Option[T]] =
    select[T](params:_*)(f) flatMap { rows => Future.value(rows.headOption) }
}
