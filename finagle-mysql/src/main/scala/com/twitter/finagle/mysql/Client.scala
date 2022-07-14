package com.twitter.finagle.mysql

import com.twitter.concurrent.AsyncMutex
import com.twitter.finagle.Status
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.ChannelClosedException
import com.twitter.finagle.ClientConnection
import com.twitter.finagle.Service
import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.ServiceProxy
import com.twitter.logging.Logger
import com.twitter.util._
import scala.annotation.tailrec

object Client {

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
    case r => Future.exception(new IllegalStateException(s"Unsupported response to a read='$r'"))
  }

  private val ResultToOK: Result => Future[OK] = {
    case ok: OK => Future.value(ok)
    case r => Future.exception(new IllegalStateException(s"Unsupported response to a modify='$r'"))
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

  /**
   * Reserve a session for exclusive use. This can be useful when operations that require
   * connection state are to be performed, for example: transactions, locks, or cursors.
   *
   * A session's life cycle is managed through the completion of the provided Future. If a
   * session is no longer usable it can be explicitly discarded.
   *
   * @example
   * {{{
   * import com.twitter.finagle.mysql.{Client, Transactions}
   * import com.twitter.util.{Future, Throw}
   *
   * case class ReallyBadException() extends Exception
   *
   * val client: Client with Transactions = ???
   *
   * client.session { session =>
   *   val result = for {
   *     _ <- session.query("LOCK")
   *     r <- session.transaction { tx =>
   *       tx.query("...")
   *     }
   *     _ <- session.query("UNLOCK")
   *   } yield r
   *
   *   result.rescue {
   *     case e: ReallyBadException =>
   *       session.discard().flatMap(_ =>
   *         Future.exception(e)
   *       )
   *   }
   * }
   * }}}
   *
   * @see Session
   */
  def session[T](f: Client with Transactions with Session => Future[T]): Future[T]
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
 * Creates a standard implementation of the mysql client interfaces.
 *
 * Note that command interrupts are handled by `GenSerialClientDispatcher`'s
 * interrupt handler. It initiates closing the transport but does not cancel
 * the interrupted command. That is, the client's connection will be closed,
 * the server will finish executing the command, and no dependent work
 * (e.g., via `flatMap`) will run.
 *
 * @param factory the underlying factory used to checkout services from.
 * This is usually a finagle client.
 *
 * @param supportUnsigned Configures whether to support unsigned integer fields when
 * returning elements of a [[Row]]. If disabled, unsigned fields will be decoded
 * as if they were signed, potentially resulting in corruption in the case of
 * overflowing the signed representation. Because Java doesn't support unsigned
 * integer types widening may be necessary to support the unsigned variants.
 * For example, an unsigned Int is represented as a Long.
 *
 * @param statsReceiver The [[StatsReceiver]] used to export stats for this client.
 *
 * @param rollbackQuery the query used to rollback a transaction, "ROLLBACK".
 * This is customizable to allow for failure testing.
 */
private class StdClient(
  factory: ServiceFactory[Request, Result],
  supportUnsigned: Boolean,
  statsReceiver: StatsReceiver,
  rollbackQuery: String)
    extends Client
    with Transactions {

  import Client._
  import StdClient._

  def this(
    factory: ServiceFactory[Request, Result],
    supportUnsigned: Boolean,
    statsReceiver: StatsReceiver
  ) = this(factory, supportUnsigned, statsReceiver, "ROLLBACK")

  private[this] val service = factory.toService

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
      // PrepareRequests and Responses are cached per-connection. Unique prepare requests are
      // issued and update cache. See [[com.twitter.finagle.mysql.PrepareCache]]
      svc(PrepareRequest(sql))
        .flatMap {
          case ok: PrepareOK => svc(ExecuteRequest(ok.id, ps.toIndexedSeq))
          case r =>
            Future.exception(new IllegalStateException(s"Unexpected result $r when preparing $sql"))
        }.ensure {
          svc.close()
        }
    }
  }

  def cursor(sql: String): CursoredStatement = {
    new CursoredStatement {

      private[this] val cursorStats = new CursorStats(statsReceiver)

      def apply[T](
        rowsPerFetch: Int,
        params: Parameter*
      )(
        f: (Row) => T
      ): Future[CursorResult[T]] = {
        assert(rowsPerFetch > 0, s"rowsPerFetch must be positive: $rowsPerFetch")

        factory().map { svc =>
          new StdCursorResult[T](cursorStats, svc, sql, rowsPerFetch, params, f, supportUnsigned)
        }
      }
    }
  }

  protected def session(): Future[Client with Transactions with Session] = factory().map { svc =>
    def singleton: ServiceFactory[Request, Result] = new ServiceFactory[Request, Result] {
      // This mutex is used to ensure that the `svc` is used in only a single context at a time.
      // This is important during the execution of a `PrepareStatement` - to guarantee that
      // the `PrepareOk` result is still open on the server when using the `svc` to execute the
      // `ExecuteRequest` operation.
      val asyncMutex = new AsyncMutex()

      private def proxy(): Future[Service[Request, Result]] = {
        asyncMutex.acquire().map { permit =>
          new ServiceProxy(svc) {

            // Because the `proxy` is used in the context of a `FactoryToService` we override
            // `Service#close` to ensure that we can control the checkout lifetime of the `Service`.
            override def close(deadline: Time): Future[Unit] = {
              permit.release()
              Future.Done
            }
          }
        }
      }

      def apply(conn: ClientConnection): Future[Service[Request, Result]] = proxy()

      def close(deadline: Time): Future[Unit] = svc.close(deadline)

      def status: Status = svc.status
    }

    val client =
      new StdClient(singleton, supportUnsigned, statsReceiver, rollbackQuery) with Session {

        // This provides continuing support for nested transactions.  The nested transaction
        // requires a new mutex to gain access to the underlying `svc`.   The parent transaction
        // cannot use the underlying `svc` until the nested transaction is complete.
        // Note, this only provides support for 1 level of nested transactions.
        override protected def session(): Future[Client with Transactions with Session] =
          Future.value(
            new StdClient(singleton, supportUnsigned, statsReceiver, rollbackQuery) with Session {
              def discard(): Future[Unit] = {
                singleton().flatMap { svc => svc(PoisonConnectionRequest) }.unit
              }

              override def session(): Future[Client with Transactions with Session] =
                Future.exception(
                  new IllegalStateException("Multiple nested transactions are not supported"))

              override def close(deadline: Time): Future[Unit] = Future.Done
            })

        def discard(): Future[Unit] = {
          singleton().flatMap { svc => svc(PoisonConnectionRequest) }.unit
        }
      }

    client
  }

  def session[T](f: Client with Transactions with Session => Future[T]): Future[T] = {
    session().flatMap { client =>
      f(client).transform { r =>
        client.close()
        Future.const(r)
      }
    }
  }

  def transactionWithIsolation[T](
    isolationLevel: IsolationLevel
  )(
    f: Client => Future[T]
  ): Future[T] = {
    transact(Some(isolationLevel), f)
  }

  def transaction[T](f: Client => Future[T]): Future[T] = {
    transact(None, f)
  }

  private[this] def transact[T](
    isolationLevel: Option[IsolationLevel],
    f: Client => Future[T]
  ): Future[T] = {
    session { client =>
      val result = for {
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

      // handle the result of the transaction, do not sequence closing/discarding the session
      // instead perform them in parallel and allow control to return to the user as soon as
      // possible. The trade off is that the close/discard takes longer than expected and the user
      // is caught waiting for a new session.
      result.transform {
        case Return(r) =>
          Future.value(r)
        case Throw(e @ WrappedChannelClosedException()) =>
          // We don't attempt a rollback in the event of a [[ChannelClosedException]]; the rollback
          // would simply fail with the same exception.
          Future.exception(e)
        case Throw(e) =>
          // the rollback is `masked` in order to protect it from prior interrupts/raises.
          // this statement should run regardless.
          client.query(rollbackQuery).masked.transform {
            case Return(_) =>
              Future.exception(e)
            case Throw(e @ WrappedChannelClosedException()) =>
              Future.exception(e)
            case Throw(rollbackEx) =>
              log.info(
                rollbackEx,
                s"Rolled back due to $e. Failed during rollback, closing " +
                  "connection"
              )
              // the rollback failed and we don't want any uncommitted state to leak
              // to the next usage of the connection. issue a "poisoned" request to close
              // the underlying connection. this is necessary due to the connection
              // pooling underneath. then, close the service.
              client.discard().transform { _ => Future.exception(e) }
          }
      }
    }
  }

  def close(deadline: Time): Future[Unit] = factory.close(deadline)
}
