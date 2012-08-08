package com.twitter.finagle.mysql

import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.mysql.protocol._
import com.twitter.finagle.mysql.util.Query
import com.twitter.finagle.Service
import com.twitter.finagle.{ServiceFactory, Codec, CodecFactory}
import com.twitter.util.Future

object Client {
  /**
   * Constructs a Client given a ServiceFactory.
   */
  def apply(factory: ServiceFactory[Request, Result]): Client = {
    new Client(factory)
  }

  /**
   * Constructs a ServiceFactory using a single host.
   * @param host a String of host:port combination.
   * @param username the username used to authenticate to the mysql instance
   * @param password the password used to authenticate to the mysql instance
   * @param dbname database to initially use
   */
  def apply(host: String, username: String, password: String, dbname: Option[String]): Client = {
    val factory = ClientBuilder()
      .codec(new MySQL(username, password, dbname))
      .hosts(host)
      .hostConnectionLimit(1)
      .buildFactory()

      apply(factory)
  }

  def apply(host: String, username: String, password: String): Client = {
    apply(host, username, password, None)
  }

  def apply(host: String, username: String, password: String, dbname: String): Client = {
    apply(host, username, password, Some(dbname))
  }
}

class Client(factory: ServiceFactory[Request, Result]) {
  private[this] lazy val fService = factory.apply()

  /**
   * Sends a query to the server without using
   * prepared statements.
   * @param sql An sql statement to be executed.
   * @return an OK Result or a ResultSet for queries that return
   * rows.
   */
  def query(sql: String) = send(QueryRequest(sql)) {
      case rs: ResultSet => Future.value(rs)
      case ok: OK => Future.value(ok)
  }

  /**
   * Runs a query that returns a result set. For each row
   * in the ResultSet, call f on the row and return the results.
   * @param sql A sql statement that returns a result set.
   * @param f A function from ResultSet to any type T.
   * @return a Future of Seq[T]
   */
  def select[T](sql: String)(f: Row => T): Future[Seq[T]] = query(sql) map {
    case rs: ResultSet => rs.rows.map(f)
    case ok: OK => Seq()
  }
  
  /**
   * Sends a query to server to be prepared for execution.
   * @param sql A query to be prepared on the server.
   * @return PreparedStatement 
   */
  def prepare(sql: String, params: Any*) = {
    val stmt = Query.expandParams(sql, params)
    send(PrepareRequest(stmt)) {
      case ps: PreparedStatement =>
        ps.statement.setValue(stmt)
        if(params.size > 0)
          ps.parameters = Query.flatten(params).toArray

        Future.value(ps)
    } 
  }

  /**
   * Execute a prepared statement.
   * @return an OK Result or a ResultSet for queries that return
   * rows.
   */
  def execute(ps: PreparedStatement) = send(ExecuteRequest(ps)) {
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
  def prepareAndExecute(sql: String, params: Any*) = 
    prepare(sql, params: _*) flatMap { ps => 
      execute(ps) map {
        res => (ps, res)
      }
    }


  /**
   * Runs a query that returns a result set. For each row
   * in the ResultSet, call f on the row and return the results.
   * @param ps A prepared statement.
   * @param f A function from ResultSet to any type T.
   * @return a Future of Seq[T]
   */
  def select[T](ps: PreparedStatement)(f: Row => T): Future[Seq[T]] = execute(ps) map {
    case rs: ResultSet => rs.rows.map(f)
    case ok: OK => Seq()
  }

  /**
   * Combines the prepare and select operations.
   * @return a Future[(PreparedStatement, Seq[T])] tuple.
   */
  def prepareAndSelect[T](sql: String, params: Any*)(f: Row => T) = 
    prepare(sql, params: _*) flatMap { ps => 
      select(ps)(f) map { 
          seq => (ps, seq)
      } 
    }

  /**
   * Close a prepared statement on the server.
   * @return OK result.
   */
  def closeStatement(ps: PreparedStatement) = send(CloseRequest(ps)) {
    case ok: OK => Future.value(ok)
  }

  def selectDB(schema: String) = send(UseRequest(schema)) {
    case ok: OK => Future.value(ok)
  }

  def ping = send(PingRequest) {
    case ok: OK => Future.value(ok)
  }

  /**
   * Close the ServiceFactory and its underlying resources.
   */
  def close() = factory.close()

  /**
   * Helper function to send requests to the ServiceFactory
   * and handle Error responses from the server.
   */
  private[this] def send[T](r: Request)(handler: PartialFunction[Result, Future[T]])  = 
    fService flatMap { service =>
      service(r) flatMap (handler orElse {
        case Error(c, s, m) => Future.exception(ServerError(c + " - " + m))
        case result         => Future.exception(ClientError("Unhandled result from server: " + result))
      })
    }
}
