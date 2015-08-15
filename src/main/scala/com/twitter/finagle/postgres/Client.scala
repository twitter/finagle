package com.twitter.finagle.postgres

import com.twitter.finagle.{Service, ServiceFactory}
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.postgres.codec.{CustomOIDProxy, PgCodec, Errors}
import com.twitter.finagle.postgres.messages._
import com.twitter.finagle.postgres.values.{StringValueEncoder, ValueParser, Value}
import com.twitter.logging.Logger
import com.twitter.util.Future

import java.util.concurrent.atomic.AtomicInteger

import org.jboss.netty.buffer.ChannelBuffer

import scala.util.Random

/*
 * A Finagle client for communicating with Postgres.
 */
class Client(factory: ServiceFactory[PgRequest, PgResponse], id:String) {
  private[this] val counter = new AtomicInteger(0)
  private[this] lazy val customTypes = CustomOIDProxy.serviceOIDMap(id)
  private[this] val logger = Logger(getClass.getName)

  /*
   * Issue an arbitrary SQL query and get the response.
   */
  def query(sql: String): Future[QueryResponse] = sendQuery(sql) {
    case SelectResult(fields, rows) => Future(ResultSet(fields, rows, customTypes))
    case CommandCompleteResponse(affected) => Future(OK(affected))
  }

  /*
   * Issue a single SELECT query and get the response.
   */
  def fetch(sql: String): Future[SelectResult] = sendQuery(sql) {
    case rs: SelectResult => Future(rs)
  }

  /*
   * Execute an update command (e.g., INSERT, DELETE) and get the response.
   */
  def executeUpdate(sql: String): Future[OK] = sendQuery(sql) {
    case CommandCompleteResponse(rows) => Future(OK(rows))
  }

  /*
   * Run a single SELECT query and wrap the results with the provided function.
   */
  def select[T](sql: String)(f: Row => T): Future[Seq[T]] = fetch(sql) map {
    rs =>
      extractRows(rs).map(f)
  }

  /*
   * Issue a single, prepared SELECT query and wrap the response rows with the provided function.
   */
  def prepareAndQuery[T](sql: String, params: Any*)(f: Row => T): Future[Seq[T]] = {
    val preparedStatement = factory.apply().flatMap {
      service =>
        parse(sql, Some(service)).map { name =>
          new PreparedStatementImpl(name, service)
        }
    }

    preparedStatement.flatMap {
      statement =>
        statement.select(params: _*)(f).ensure {
          statement.closeService
        }
    }
  }

  /*
   * Issue a single, prepared arbitrary query without an expected result set, and provide the affected row count
   */
  def prepareAndExecute(sql: String, params: Any*):Future[Int] = {
    val preparedStatement = factory.apply().flatMap {
      service =>
        parse(sql, Some(service)).map { name =>
	  new PreparedStatementImpl(name, service)
	}
    }

    preparedStatement.flatMap {
      statement =>
        statement.exec(params: _*).ensure {
	  statement.closeService
	}
    } map {
      case OK(count) => count
    }
  }

  private[this] def sendQuery[T](sql: String)(handler: PartialFunction[PgResponse, Future[T]]) = {
    send(PgRequest(new Query(sql)))(handler)
  }

  private[this] def parse(
      sql: String,
      optionalService: Option[Service[PgRequest, PgResponse]] = None): Future[String] = {
    val name = genName()

    send(PgRequest(Parse(name, sql), flush = true), optionalService) {
      case ParseCompletedResponse => Future.value(name)
    }
  }

  private[this] def bind(
      name: String,
      params: Seq[ChannelBuffer] = Seq(),
      optionalService: Option[Service[PgRequest, PgResponse]] = None): Future[Unit] = {
    send(PgRequest(Bind(portal = name, name = name, params = params), flush = true), optionalService) {
      case BindCompletedResponse => Future.value(())
    }
  }

  private[this] def describe(
      name: String,
      optionalService: Option[Service[PgRequest, PgResponse]] = None
    ): Future[(IndexedSeq[String], IndexedSeq[ChannelBuffer => Value[Any]])] = {
    send(PgRequest(Describe(portal = true, name = name), flush = true), optionalService) {
      case RowDescriptions(fields) => Future.value(processFields(fields))
    }
  }

  private[this] def execute(
      name: String,
      maxRows: Int = 0,
      optionalService: Option[Service[PgRequest, PgResponse]] = None) = {
    fire(PgRequest(Execute(name, maxRows), flush = true), optionalService)
  }

  private[this] def sync(
      optionalService: Option[Service[PgRequest, PgResponse]] = None): Future[Unit] = {
    send(PgRequest(Sync), optionalService) {
      case ReadyForQueryResponse => Future.value(())
    }
  }

  private[this] def fire(r: PgRequest, optionalService: Option[Service[PgRequest, PgResponse]] = None) = {
    optionalService match {
      case Some(service) =>
        // A service has been passed in; use it
        service.apply(r)
      case _ =>
        // Create a new service instance from the client factory
        factory.toService(r)
    }
  }

  private[this] def send[T](
      r: PgRequest,
      optionalService: Option[Service[PgRequest, PgResponse]] = None
    )(handler: PartialFunction[PgResponse, Future[T]]) = {
    fire(r, optionalService) flatMap (handler orElse {
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

  private[this] class PreparedStatementImpl(
      name: String,
      service: Service[PgRequest, PgResponse]) extends PreparedStatement {
    def closeService = service.close()

    override def fire(params: Any*): Future[QueryResponse] = {
      val binaryParams = params.map {
        p => StringValueEncoder.encode(p)
      }

      val f = for {
        _ <- bind(name, binaryParams, Some(service))
        (fieldNames, fieldParsers) <- describe(name, Some(service))
        exec <- execute(name, optionalService = Some(service))
      } yield exec match {
          case CommandCompleteResponse(rows) => OK(rows)
          case Rows(rows, true) => ResultSet(fieldNames, fieldParsers, rows, customTypes)
        }
      f transform {
        result =>
          sync(Some(service)).flatMap {
            _ => Future.const(result)
          }
      }
    }
  }

  private[this] def genName() = "fin-pg-" + counter.incrementAndGet
}

/*
 * Helper companion object that generates a client from authentication information.
 */
object Client {
  def apply(
      host: String,
      username: String,
      password: Option[String],
      database: String,
      useSsl: Boolean = false,
      hostConnectionLimit: Int = 1,
      numRetries: Int = 4,
      customTypes: Boolean = false): Client = {
    val id = Random.alphanumeric.take(28).mkString

    val factory: ServiceFactory[PgRequest, PgResponse] = ClientBuilder()
      .codec(new PgCodec(username, password, database, id, useSsl = useSsl, customTypes = customTypes))
      .hosts(host)
      .hostConnectionLimit(hostConnectionLimit)
      .retries(numRetries)
      .failFast(enabled = true)
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
    case OK(_) => Seq.empty[Row].map(f)
  }

  def selectFirst[T](params: Any*)(f: Row => T): Future[Option[T]] =
    select[T](params:_*)(f) flatMap { rows => Future.value(rows.headOption) }
}
