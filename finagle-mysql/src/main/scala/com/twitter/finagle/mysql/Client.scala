package com.twitter.finagle.exp.mysql

import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver}
import com.twitter.util.{Closable, Future, Time}
import java.util.logging.{Logger, Level}
import com.twitter.finagle.exp.Mysql

class ClientError(msg: String) extends Exception(msg)

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
  @deprecated("Use the com.twitter.finagle.exp.Mysql object to build a client", "6.6.2")
  def apply(
    host: String,
    username: String,
    password: String,
    dbname: String = null,
    logLevel: Level = Level.OFF,
    statsReceiver: StatsReceiver = NullStatsReceiver
  ): Client = {
    logger.setLevel(logLevel)

    val factory = Mysql
      .withCredentials(username, password)
      .withDatabase(dbname)
      .newClient(host)

      apply(factory)
  }
}

class Client(val factory: ServiceFactory[Request, Result]) extends Closable {
  private[this] val service = factory.toService

  /**
   * Sends a query without using prepared statements.
   *
   * @param sql the sql string to be sent to the server
   * @return a Future containing the Result object from the query.
   */
  def query(sql: String): Future[Result] =
    service(QueryRequest(sql))

  /**
   * Sends a query that presumably returns a ResultSet.
   * Each row is mapped to f, a function from Row => T.
   *
   * @param sql A sql statement that returns a result set.
   * @param f A function from Row to any type T.
   * @return a Future of Seq[T] that contains the result
   * of f applied to each row.
   */
  def select[T](sql: String)(f: Row => T): Future[Seq[T]] =
    query(sql) map {
      case rs: ResultSet => rs.rows.map(f)
      case _ => Seq.empty
    }

  /**
   * Sends a query to server to be prepared for execution.
   * @param sql A query to be prepared on the server.
   * @return a Future[PreparedStatement]
   */
  def prepare(sql: String): Future[PreparedStatement] =
    service(PrepareRequest(sql)) flatMap {
      case ok: PrepareOK =>
        Future.value(new PreparedStatement(ok))
      case result =>
        Future.exception(
          new ClientError("Unexpected result: %s".format(result))
        )
    }

  /**
   * Execute a prepared statement on the server. Set the parameters
   * field or use the update(index, param) method on the
   * prepared statement object to change parameters.
   * @return a Future containing an OK Result or a ResultSet
   * for queries that return rows.
   */
  def execute(ps: PreparedStatement): Future[Result] =
    service(ExecuteRequest(ps)) flatMap {
      case rs: ResultSet =>
        ps.bindParameters()
        Future.value(rs)
      case ok: OK =>
        ps.bindParameters()
        Future.value(ok)
      case result =>
        Future.value(result)
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
  def select[T](ps: PreparedStatement)(f: Row => T): Future[Seq[T]] =
    execute(ps) map {
      case rs: ResultSet => rs.rows.map(f)
      case _ => Seq()
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
   * @return a Future with an OK value if successful.
   */
  def closeStatement(ps: PreparedStatement): Future[Result] =
    service(CloseRequest(ps.statementId))

  /**
   * Changes the default schema.
   *
   * @param schema The new schema to use.
   * @return a Future with an OK value if successful.
   */
  def selectDB(schema: String): Future[Result] =
    service(UseRequest(schema))

  /**
   * Checks if the server is alive.
   *
   * @return a Future with an OK value if the
   * server is alive.
   */
  def ping: Future[Result] =
    service(PingRequest)

  /**
   * Close the ServiceFactory and its underlying resources.
   */
  def close(deadline: Time): Future[Unit] = factory.close(deadline)
}