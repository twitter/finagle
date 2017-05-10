package com.twitter.finagle.mysql

import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.{ClientConnection, ServiceFactory, ServiceProxy}
import com.twitter.util._

object Client {
  /**
   * Creates a new Client based on a ServiceFactory.
   */
  def apply(
    factory: ServiceFactory[Request, Result],
    statsReceiver: StatsReceiver = NullStatsReceiver
  ): Client with Transactions with Cursors = new StdClient(factory, statsReceiver)
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
    * @note we use a ServiceFactory that returns the same Service repeatedly to the client. This is
   * to assure that a new MySQL connection (i.e. Service) from the connection pool (i.e.,
   * ServiceFactory) will be used for each new transaction. Only upon completion of the transaction
   * is the connection returned to the pool for re-use.
   */
  def transaction[T](f: Client => Future[T]): Future[T]
}

trait Cursors {
  /**
   * Create a CursoredStatement with the given parameterized sql query.
   * The returned cursored statement can be reused and applied with varying
   * parameters.
   *
   * @note The cursored statements are built on a prepare -> execute -> fetch flow
   * that requires state tracking. It is important to either fully consume the resulting
   * stream, or explicitly call `close()`
   */
  def cursor(sql: String): CursoredStatement
}

private class StdClient(factory: ServiceFactory[Request, Result], statsReceiver: StatsReceiver)
  extends Client with Transactions with Cursors {

  private[this] val service = factory.toService

  private[this] val cursorStats = new CursorStats(statsReceiver)

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

  def cursor(sql: String): CursoredStatement = {
    new CursoredStatement {
      override def apply[T](rowsPerFetch: Int, params: Parameter*)(f: (Row) => T): Future[CursorResult[T]] = {
        assert(rowsPerFetch > 0, "rowsPerFetch must be positive")

        factory().map { svc =>
          new StdCursorResult[T](cursorStats, svc, sql, rowsPerFetch, params, f)
        }
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

    val client = Client(singleton, statsReceiver)
    val transaction = for {
      _ <- client.query("START TRANSACTION")
      result <- f(client)
      _ <- client.query("COMMIT")
    } yield {
      result
    }

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
