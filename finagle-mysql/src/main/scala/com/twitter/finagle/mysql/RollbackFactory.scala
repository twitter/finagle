package com.twitter.finagle.mysql

import com.twitter.finagle._
import com.twitter.finagle.param.Stats
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.logging.Logger
import com.twitter.util.Future
import com.twitter.util.Return
import com.twitter.util.Stopwatch
import com.twitter.util.Throw
import com.twitter.util.Time

object RollbackFactory {
  private val RollbackQuery = QueryRequest("ROLLBACK")

  private val log = Logger.get()

  val Role: Stack.Role = Stack.Role("RollbackFactory")

  private[finagle] def module: Stackable[ServiceFactory[Request, Result]] =
    new Stack.Module1[Stats, ServiceFactory[Request, Result]] {
      val role: Stack.Role = Role
      val description: String = "Installs a rollback factory in the stack"
      def make(
        sr: Stats,
        next: ServiceFactory[Request, Result]
      ): ServiceFactory[Request, Result] = {
        new RollbackFactory(next, sr.statsReceiver)
      }
    }
}

/**
 * A `ServiceFactory` that ensures a ROLLBACK statement is issued when a service is put
 * back into the connection pool.
 *
 * @see https://dev.mysql.com/doc/en/implicit-commit.html
 */
final class RollbackFactory(client: ServiceFactory[Request, Result], statsReceiver: StatsReceiver)
    extends ServiceFactoryProxy(client) {
  import RollbackFactory._

  private[this] val rollbackLatencyStat = statsReceiver.stat(s"rollback_latency_ms")

  private[this] def wrap(underlying: Service[Request, Result]): Service[Request, Result] =
    new ServiceProxy[Request, Result](underlying) {

      override def close(deadline: Time): Future[Unit] = {
        if (self.status == Status.Closed) poisonAndClose(deadline)
        else rollback(deadline)
      }

      private[this] def rollback(deadline: Time): Future[Unit] = {
        val elapsed = Stopwatch.start()
        // the rollback is `masked` in order to protect it from prior interrupts/raises.
        // this statement should run regardless.
        self(RollbackQuery).masked.transform { result =>
          rollbackLatencyStat.add(elapsed().inMillis)
          result match {
            case Return(_) => self.close(deadline)
            case Throw(_: ChannelClosedException) | Throw(_: ChannelWriteException) =>
              // Don't log the exception on ChannelClosedExceptions/ChannelWriteException
              // because it is noisy.

              // We want to close the connection if we can't issue a rollback
              // since we assume it isn't a "clean" connection to put back into
              // the pool.
              poisonAndClose(deadline)
            case Throw(t) =>
              log.warning(
                t,
                "rollback failed when putting service back into pool, closing connection"
              )
              // We want to close the connection if we can't issue a rollback
              // since we assume it isn't a "clean" connection to put back into
              // the pool.
              poisonAndClose(deadline)
          }
        }
      }

      private[this] def poisonAndClose(deadline: Time): Future[Unit] = {
        // the poison req is `masked` in order to protect it from prior interrupts/raises.
        // this statement should run regardless.
        self(PoisonConnectionRequest).masked.transform { _ => self.close(deadline) }
      }
    }

  private[this] val wrapFn: Service[Request, Result] => Service[Request, Result] = { svc =>
    wrap(svc)
  }

  override def apply(conn: ClientConnection): Future[Service[Request, Result]] =
    super.apply(conn).map(wrapFn)
}
