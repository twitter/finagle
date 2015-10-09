package com.twitter.finagle.exp.mysql

import java.util.logging.Level
import com.twitter.finagle.{ServiceProxy, ClientConnection, ServiceFactory}
import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.util._

object Client {
  /**
   * Creates a new Client based on a ServiceFactory.
   */
  def apply(factory: ServiceFactory[Request, Result]): Client with Transactions = new StdClient(factory)

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
    val factory = com.twitter.finagle.exp.Mysql.client
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

trait Transactions {
  /**
   * Execute `f` in a transaction.
   *
   * If `f` throws an exception, the transaction is rolled back. Otherwise, the transaction is
   * committed.
   *
   * @example {{{
   *   client.transaction[Foo] { c =>
   *    for {
   *       r0 <- c.query(q0)
   *       r1 <- c.query(q1)
   *       response: Foo <- buildResponse(r1, r2)
   *     } yield response
   *   }
   * }}}
   *
   * @note we use a ServiceFactory that returns the same Service repeatedly to the client. This is
   * to assure that a new MySQL connection (i.e. Service) from the connection pool (i.e.,
   * ServiceFactory) will be used for each new transaction. Only upon completion of the transaction
   * is the connection returned to the pool for re-use.
   */
  def transaction[T](f: Client => Future[T]): Future[T]
}

private[mysql] class StdClient(factory: ServiceFactory[Request, Result])
  extends Client with Transactions {
  private[this] val service = factory.toService

  def query(sql: String): Future[Result] = service(QueryRequest(sql))
  def ping(): Future[Result] = service(PingRequest)

  def select[T](sql: String)(f: Row => T): Future[Seq[T]] =
    query(sql) map {
      case rs: ResultSet => rs.rows.map(f)
      case _ => Nil
    }

  def prepare(sql: String): PreparedStatement = new PreparedStatement {
    def apply(ps: Parameter*): Future[Result] = factory() flatMap { svc =>
      svc(PrepareRequest(sql)).flatMap {
        case ok: PrepareOK => svc(ExecuteRequest(ok.id, ps.toIndexedSeq))
        case r => Future.exception(new Exception("Unexpected result %s when preparing %s"
          .format(r, sql)))
      } ensure {
        svc.close()
      }
    }
  }

  def transaction[T](f: Client => Future[T]): Future[T] = {
    val singleton = new ServiceFactory[Request, Result] {
      val svc = factory()
      // Because the `singleton` is used in the context of a `FactoryToService` we override
      // `Service#close` to ensure that we can control the checkout lifetime of the `Service`.
      val proxiedService = svc map { service =>
        new ServiceProxy(service) {
          override def close(deadline: Time) = Future.Done
        }
      }

      def apply(conn: ClientConnection) = proxiedService
      def close(deadline: Time): Future[Unit] = svc.flatMap(_.close(deadline))
    }

    val client = Client(singleton)
    val transaction = for {
      _ <- client.query("START TRANSACTION")
      result <- f(client)
      _ <- client.query("COMMIT")
    } yield result

    // handle failures and put connection back in the pool

    transaction transform {
      case Return(r) =>
        singleton.close()
        Future.value(r)
      case Throw(e) =>
        client.query("ROLLBACK") transform { _ =>
          singleton.close()
          Future.exception(e)
        }
    }
  }

  def close(deadline: Time): Future[Unit] = factory.close(deadline)
}
