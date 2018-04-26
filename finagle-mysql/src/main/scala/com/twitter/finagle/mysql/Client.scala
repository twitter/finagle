package com.twitter.finagle.mysql

import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.{ChannelClosedException, ClientConnection, Service, ServiceFactory, ServiceProxy}
import com.twitter.logging.Logger
import com.twitter.util._
import scala.annotation.tailrec

object Client {

  /**
   * Creates a new Client based on a ServiceFactory.
   *
   * @note the returned `Client` will *not* include support for unsigned integer types.
   */
  @deprecated("Use the three argument constructor instead.", "2017-08-11")
  def apply(
    factory: ServiceFactory[Request, Result],
    statsReceiver: StatsReceiver = NullStatsReceiver
  ): Client with Transactions = apply(factory, statsReceiver, supportUnsigned = false)

  /**
   * Creates a new Client based on a ServiceFactory.
   */
  def apply(
    factory: ServiceFactory[Request, Result],
    statsReceiver: StatsReceiver,
    supportUnsigned: Boolean
  ): Client with Transactions = new StdClient(factory, supportUnsigned, statsReceiver)

  // Extractor for nested [[ChannelClosedException]]s. Returns true if exception contains a
  // [[ChannelClosedException]], and false otherwise.
  //
  // Exposed for testing.
  private[mysql] object WrappedChannelClosedException {
    @tailrec
    def unapply(t: Throwable): Boolean = t match {
      case _: ChannelClosedException => true
      case _: Throwable => unapply(t.getCause)
      case _ => false
    }
  }

  private val ResultToResultSet: Result => Future[ResultSet] = {
    case rs: ResultSet => Future.value(rs)
    case r => Future.exception(
      new IllegalStateException(s"Unsupported response to a read='$r'"))
  }

  private val ResultToOK: Result => Future[OK] = {
    case ok: OK => Future.value(ok)
    case r => Future.exception(
      new IllegalStateException(s"Unsupported response to a modify='$r'"))
  }
}

/**
 * A MySQL client that is not `Service`-based like [[com.twitter.finagle.Mysql.Client]] is,
 * making it easier to use for most cases.
 *
 * @example Creation:
 * {{{
 * import com.twitter.finagle.Mysql
 * import com.twitter.finagle.mysql.Client
 *
 * val client: Client = Mysql.client
 *   .withCredentials("username", "password")
 *   .withDatabase("database")
 *   .newRichClient("host:port")
 * }}}
 */
trait Client extends Closable {

  /**
   * Returns the result of executing the `sql` query on the server.
   *
   * '''Note:''' this is a lower-level API. For SELECT queries, prefer using
   * [[select]] or [[read]], and use [[modify]] for DML (INSERT/UPDATE/DELETE)
   * and DDL.
   *
   * @return a [[Result]] `Future`. A successful SELECT query will satisfy the
   *         `Future` with a [[ResultSet]] while DML and DDL will be an [[OK]].
   *         If there is a server error the `Future` will be failed with a [[ServerError]]
   *         exception, '''not''' an [[Error]] from the [[Result]] ADT.
   */
  def query(sql: String): Future[Result]

  /**
   * Executes the given SELECT query given by `sql`.
   *
   * @example
   * {{{
   * import com.twitter.finagle.mysql.{Client, ResultSet}
   * import com.twitter.util.Future
   *
   * val client: Client = ???
   * val resultSet: Future[ResultSet] =
   *   client.read("SELECT name FROM employee")
   * }}}
   *
   * @see [[select]]
   * @see [[PreparedStatement.read]]
   */
  def read(sql: String): Future[ResultSet] =
    query(sql).flatMap(Client.ResultToResultSet)

  /**
   * Executes the given DML (e.g. INSERT/UPDATE/DELETE) or DDL
   * (e.g. CREATE TABLE, DROP TABLE, COMMIT, START TRANSACTION, etc)
   * given by `sql`.
   *
   * @example
   * {{{
   * import com.twitter.finagle.mysql.{Client, OK}
   * import com.twitter.util.Future
   *
   * val client: Client = ???
   * val result: Future[OK] =
   *   client.modify("INSERT INTO employee (name) VALUES ('Alice')")
   * }}}
   *
   * @see [[PreparedStatement.modify]]
   */
  def modify(sql: String): Future[OK] =
    query(sql).flatMap(Client.ResultToOK)

  /**
   * Sends the given `sql` to the server and maps each resulting row to
   * `f`, a function from Row => T. If no ResultSet is returned, the function
   * returns an empty Seq.
   *
   * @example
   * {{{
   * import com.twitter.finagle.mysql.Client
   * import com.twitter.util.Future
   *
   * val client: Client = ???
   * val names: Future[Seq[String]] =
   *   client.select("SELECT name FROM employee") { row =>
   *     row.stringOrNull("name")
   *   }
   * }}}
   *
   * @see [[read]]
   * @see [[PreparedStatement.select]]
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
   * Create a CursoredStatement with the given parameterized sql query.
   * The returned cursored statement can be reused and applied with varying
   * parameters.
   *
   * @note The cursored statements are built on a prepare -> execute -> fetch flow
   * that requires state tracking. It is important to either fully consume the resulting
   * stream, or explicitly call [[CursorResult.close()]].
   */
  def cursor(sql: String): CursoredStatement

  /**
   * Returns the result of pinging the server.
   */
  def ping(): Future[Unit]
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

  /**
   * Execute `f` in a transaction using the given Isolation Level for this transaction only.
   * This Isolation Level overrides the session and global database settings for the transaction.
   *
   * If `f` throws an exception, the transaction is rolled back. Otherwise, the transaction is
   * committed.
   *
   * @example {{{
   *   client.transaction[Foo](IsolationLevel.RepeatableRead) { c =>
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
  def transactionWithIsolation[T](isolationLevel: IsolationLevel)(f: Client => Future[T]): Future[T]
}

private object StdClient {
  val log: Logger = Logger.get()
}

/**
 * @param rollbackQuery the query used to rollback a transaction, "ROLLBACK".
 *                      This is customizable to allow for failure testing.
 */
private class StdClient(
  factory: ServiceFactory[Request, Result],
  supportUnsigned: Boolean,
  statsReceiver: StatsReceiver,
  rollbackQuery: String
) extends Client
    with Transactions {

  import StdClient._
  import Client._

  def this(
    factory: ServiceFactory[Request, Result],
    supportUnsigned: Boolean,
    statsReceiver: StatsReceiver
  ) = this(factory, supportUnsigned, statsReceiver, "ROLLBACK")

  private[this] val service = factory.toService

  private[this] val cursorStats = new CursorStats(statsReceiver)

  def query(sql: String): Future[Result] = service(QueryRequest(sql))

  def ping(): Future[Unit] =
    service(PingRequest).unit

  def select[T](sql: String)(f: Row => T): Future[Seq[T]] =
    query(sql).map {
      case rs: ResultSet => rs.rows.map(f)
      case _ => Nil
    }

  def prepare(sql: String): PreparedStatement = new PreparedStatement {
    def apply(ps: Parameter*): Future[Result] = factory().flatMap { svc =>
      svc(PrepareRequest(sql)).flatMap {
        case ok: PrepareOK => svc(ExecuteRequest(ok.id, ps.toIndexedSeq))
        case r => Future.exception(new IllegalStateException(s"Unexpected result $r when preparing $sql"))
      }.ensure {
        svc.close()
      }
    }
  }

  def cursor(sql: String): CursoredStatement = {
    new CursoredStatement {
      def apply[T](rowsPerFetch: Int, params: Parameter*)(
        f: (Row) => T
      ): Future[CursorResult[T]] = {
        assert(rowsPerFetch > 0, s"rowsPerFetch must be positive: $rowsPerFetch")

        factory().map { svc =>
          new StdCursorResult[T](cursorStats, svc, sql, rowsPerFetch, params, f, supportUnsigned)
        }
      }
    }
  }

  def transactionWithIsolation[T](
    isolationLevel: IsolationLevel
  )(f: Client => Future[T]): Future[T] = {
    transact(Some(isolationLevel), f)
  }

  def transaction[T](f: Client => Future[T]): Future[T] = {
    transact(None, f)
  }

  private[this] def transact[T](
    isolationLevel: Option[IsolationLevel],
    f: Client => Future[T]
  ): Future[T] = {
    val singleton = new ServiceFactory[Request, Result] {
      private val svc: Future[Service[Request, Result]] = factory()

      // Because the `singleton` is used in the context of a `FactoryToService` we override
      // `Service#close` to ensure that we can control the checkout lifetime of the `Service`.
      private val proxiedService: Future[Service[Request, Result]] = svc.map { service =>
        new ServiceProxy(service) {
          override def close(deadline: Time): Future[Unit] = Future.Done
        }
      }

      def apply(conn: ClientConnection): Future[Service[Request, Result]] = proxiedService
      def close(deadline: Time): Future[Unit] = svc.flatMap(_.close(deadline))
    }
    val client = Client(singleton, statsReceiver, supportUnsigned)
    val transaction = for {
      _ <- isolationLevel match {
        case Some(iso) => client.query(s"SET TRANSACTION ISOLATION LEVEL ${iso.name}")
        case None => Future.Done
      }
      _ <- client.query("START TRANSACTION")
      result <- f(client)
      _ <- client.query("COMMIT")
    } yield {
      result
    }

    // handle failures and put connection back in the pool

    def closeWith(e: Throwable): Future[T] = {
      singleton.close()
      Future.exception(e)
    }

    transaction.transform {
      case Return(r) =>
        singleton.close()
        Future.value(r)
      case Throw(e @ WrappedChannelClosedException()) =>
        // We don't attempt a rollback in the event of a [[ChannelClosedException]]; the rollback
        // would simply fail with the same exception.
        closeWith(e)
      case Throw(e) =>
        // the rollback is `masked` in order to protect it from prior interrupts/raises.
        // this statement should run regardless.
        client.query(rollbackQuery).masked.transform {
          case Return(_) =>
            closeWith(e)
          case Throw(e @ WrappedChannelClosedException()) =>
            closeWith(e)
          case Throw(rollbackEx) =>
            log.info(rollbackEx, s"Rolled back due to $e. Failed during rollback, closing " +
              "connection")
            // the rollback failed and we don't want any uncommitted state to leak
            // to the next usage of the connection. issue a "poisoned" request to close
            // the underlying connection. this is necessary due to the connection
            // pooling underneath. then, close the service.
            singleton().flatMap { svc =>
              svc(PoisonConnectionRequest)
            }.transform { _ =>
              closeWith(e)
            }
        }
    }
  }

  def close(deadline: Time): Future[Unit] = factory.close(deadline)
}
