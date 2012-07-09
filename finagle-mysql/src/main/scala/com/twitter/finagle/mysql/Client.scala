package com.twitter.finagle.mysql

import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.mysql.codec.MySQL
import com.twitter.finagle.mysql.protocol._
import com.twitter.finagle.Service
import com.twitter.finagle.{ServiceFactory, Codec, CodecFactory}
import com.twitter.util.Future
import scala.collection.mutable

object Client {
  /**
   * Construct a Client given a ServiceFactory.
   */
  def apply(factory: ServiceFactory[Request, Result]): Client = {
    new Client(factory)
  }

  /**
   * Construct a ServiceFactory using a single host.
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

  class Client(factory: ServiceFactory[Request, Result]) {
    private lazy val fService = factory.apply()

    /**
     * Sends a query to the server without using
     * prepared statements.
     * @param query A query to be executed
     * @return OK result or a ResultSet for more complex queries.
     */
    def query(sql: String) = send(Query(sql)) {
      case rs: ResultSet => Future.value(rs)
      case ok: OK => Future.value(ok)
    }

    /**
     * Runs a query that returns a result set. For each row
     * in the ResultSet, call f on the row and aggregate the results.
     * @param sql A sql statement that returns a result set.
     * @param f A function from ResultSet to any type T.
     * @return a future of Seq[T]
     */
    def select[T](sql: String)(f: ResultSet => T): Future[Seq[T]] = {
      query(sql) map {
        case rs: ResultSet =>
          val results = new mutable.ArrayBuffer[T]

          while(rs.next)
            results += f(rs)

          results

        case ok: OK => Seq()
      }
    }

    //def prepareStatement(query: String) = send(PrepareStatement(query))
    //def execute(stmtId: Int) = send(ExecuteStatement(stmtId))

    def selectDB(schema: String)  = send(Use(schema)) {
      case ok: OK => Future.value(ok)
    }

    def createDB(schema: String) = send(CreateDb(schema)) {
      case ok: OK => Future.value(ok)
    }

    def dropDB(schema: String) = send(DropDb(schema)) {
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
    private def send[T](r: Request)(handler: PartialFunction[Result, Future[T]])  = 
      fService flatMap { service =>
        service(r) flatMap (handler orElse {
          case Error(c, s, m) => Future.exception(ServerError(c + " - " + m))
          case result         => Future.exception(ClientError("Unhandled result from server: " + result))
        })
      }

  }
}
