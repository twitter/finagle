package com.twitter.finagle.exp.mysql

import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.util.{Closable, Future, Time}
import java.util.logging.Level

object Client {
  /**
   * Creates a new Client based on a ServiceFactory.
   */
  def apply(factory: ServiceFactory[Request, Result]): Client = new Client {
    private[this] val service = factory.toService

    def query(sql: String): Future[Result] = service(QueryRequest(sql))
    def ping(): Future[Result] = service(PingRequest)

    def select[T](sql: String)(f: Row => T): Future[Seq[T]] =
      query(sql) map {
        case rs: ResultSet => rs.rows.map(f)
        case _ => Seq.empty
      }

    def prepare(sql: String): PreparedStatement = new PreparedStatement {
      def apply(ps: Any*): Future[Result] = factory() flatMap { svc =>
        svc(PrepareRequest(sql)) flatMap {
          case ok: PrepareOK => svc(ExecuteRequest(ok.id, ps.toIndexedSeq))
          case r => Future.exception(new Exception("Unexpected result %s when preparing %s"
            .format(r, sql)))
        } ensure {
          svc.close()
        }
      }
    }

    def close(deadline: Time): Future[Unit] = factory.close(deadline)
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
    val factory = com.twitter.finagle.exp.Mysql
      .withCredentials(username, password)
      .withDatabase(dbname)
      .newClient(host)

      apply(factory)
  }
}

trait Client extends Closable {
  /**
   * Returns the result of executing the `sql` query on the server.
   */
  def query(sql: String): Future[Result]

  /**
   * Sends the given `sql` to the server and maps each resulting row to
   * `f`, a function from Row => T. If no ResultSet is returned, the function
   * returns an empty Seq.
   */
  def select[T](sql: String)(f: Row => T): Future[Seq[T]]

  /**
   * Returns a new PreparedStatement instance based on the given sql query.
   * The returned prepared statement can be reused and applied with varying
   * parameters.
   *
   * @note Mysql prepared statements are stateful, that is, they allocate
   * resources on the mysql server. The allocations are managed by a
   * finagle-mysql connection. Closing the client implicitly closes all
   * outstanding PreparedStatements.
   */
  def prepare(sql: String): PreparedStatement

  /**
   * Returns the result of pinging the server.
   */
  def ping(): Future[Result]
}