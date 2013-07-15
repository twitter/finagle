package com.twitter.finagle.exp.mysql

import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver}
import com.twitter.util.{Closable, Future, Time}
import java.util.logging.{Logger, Level}

case class ClientError(msg: String) extends Exception(msg)
case class ServerError(code: Short, sqlState: String, message: String) extends Exception(message)
case class IncompatibleServer(msg: String) extends Exception(msg)

object Client {
  private[this] val logger = Logger.getLogger("finagle-mysql")

  /**
   * Constructs a Client given a ServiceFactory.
   */
  def apply(factory: ServiceFactory[Request, Result]): Client = {
    new Client(factory)
  }

  /**
   * Constructs a Client using a single host.
   * @param host a String of host:port combination.
   * @param username the username used to authenticate to the mysql instance
   * @param password the password used to authenticate to the mysql instance
   * @param dbname database to initially use
   * @param logLevel log level for logger used in the finagle ChannelSnooper
   * and the finagle-mysql netty pipeline.
   * @param statsReceiver collects finagle stats scoped to "mysql"
   */
  def apply(
    host: String,
    username: String,
    password: String,
    dbname: String = null,
    logLevel: Level = Level.OFF,
    statsReceiver: StatsReceiver = NullStatsReceiver
  ): Client = {
    logger.setLevel(logLevel)

    val factory = ClientBuilder()
      .codec(new MySQL(username, password, Option(dbname)))
      .logger(logger)
      .hosts(host)
      .hostConnectionLimit(1)
      .reportTo(statsReceiver.scope("mysql"))
      .buildFactory()

      apply(factory)
  }
}

class Client(val factory: ServiceFactory[Request, Result]) extends Closable {
  private[this] val service = factory.toService

  /**
   * Sends a query to the server without using
   * prepared statements.
   * @param sql A sql statement to be sent to the server.
   * @return a Future containing an OK Result or a ResultSet
   * for queries that return rows.
   */
  def query(sql: String): Future[Result] = send(QueryRequest(sql)) {
    case rs: ResultSet => Future.value(rs)
    case ok: OK => Future.value(ok)
  }

  /**
   * Send a query that presumably returns a ResultSet. For each row
   * in the ResultSet, call f on the row and return the Seq of results.
   * If the query result does not contain a ResultSet, the query is executed
   * and an empty Seq is returned.
   * @param sql A sql statement that returns a result set.
   * @param f A function from Row to any type T.
   * @return a Future of Seq[T] that contains rows.map(f)
   */
  def select[T](sql: String)(f: Row => T): Future[Seq[T]] = query(sql) map {
    case rs: ResultSet => rs.rows.map(f)
    case ok: OK => Seq()
  }

  /**
   * Sends a query to server to be prepared for execution.
   * @param sql A query to be prepared on the server.
   * @return a Future[PreparedStatement]
   */
  def prepare(sql: String): Future[PreparedStatement] = send(PrepareRequest(sql)) {
    case ps: PreparedStatement =>
      ps.statement.setValue(sql)
      Future.value(ps)
  }

  /**
   * Execute a prepared statement on the server. Set the parameters
   * field or use the update(index, param) method on the
   * prepared statement object to change parameters.
   * @return a Future containing an OK Result or a ResultSet
   * for queries that return rows.
   */
  def execute(ps: PreparedStatement): Future[Result] = send(ExecuteRequest(ps)) {
    case rs: ResultSet =>
      ps.bindParameters()
      Future.value(rs)
    case ok: OK =>
      ps.bindParameters()
      Future.value(ok)
  }

  /**
   * Combines the prepare and execute operations.
   * @return a Future[(PreparedStatement, Result)] tuple.
   */
  def prepareAndExecute(sql: String, queryParams: Any*): Future[(PreparedStatement, Result)] =
    prepare(sql) flatMap { ps =>
      ps.parameters = queryParams.toArray
      execute(ps) map {
        res => (ps, res)
      }
    }

  /**
   * Send a SELECT query that returns a ResultSet. For each row
   * in the ResultSet, call f on the row and return the results.
   * If a prepared statement that does not represent a SELECT
   * query is passed in, an empty Seq is returned.
   * @param ps A prepared statement.
   * @param f A function from Row to any type T.
   * @return a Future of Seq[T] that contains rows.map(f)
   */
  def select[T](ps: PreparedStatement)(f: Row => T): Future[Seq[T]] = execute(ps) map {
    case rs: ResultSet => rs.rows.map(f)
    case ok: OK => Seq()
  }

  /**
   * Combines the prepare and select operations.
   * @return a Future[(PreparedStatement, Seq[T])] tuple.
   */
  def prepareAndSelect[T](sql: String, queryParams: Any*)(f: Row => T): Future[(PreparedStatement, Seq[T])] =
    prepare(sql) flatMap { ps =>
      ps.parameters = queryParams.toArray
      select(ps)(f) map {
          seq => (ps, seq)
      }
    }

  /**
   * Close a prepared statement on the server.
   * @return OK result.
   */
  def closeStatement(ps: PreparedStatement): Future[Result] = send(CloseRequest(ps)) {
    case ok: OK => Future.value(ok)
  }

  def selectDB(schema: String): Future[Result] = send(UseRequest(schema)) {
    case ok: OK => Future.value(ok)
  }

  def ping: Future[Result] = send(PingRequest) {
    case ok: OK => Future.value(ok)
  }

  /**
   * Close the ServiceFactory and its underlying resources.
   */
  def close(deadline: Time): Future[Unit] = factory.close(deadline)

  /**
   * Helper function to send requests to the ServiceFactory
   * and handle Error responses from the server.
   */
  private[this] def send[T](r: Request)(handler: PartialFunction[Result, Future[T]]): Future[T] =
    service(r) flatMap (handler orElse {
      case Error(c, s, m)       => Future.exception(ServerError(c, s, m))
      case result               => Future.exception(ClientError("Unhandled result from server: " + result))
    })
}