package com.twitter.finagle.mysql

import com.twitter.finagle._
import com.twitter.util.Future
import com.twitter.logging.{HasLogLevel, Level}
import scala.util.control.NoStackTrace

/**
 * A stack module that allows permanently closing a service.
 *
 * This is a workaround for connection pooling that allows us to close a connection.
 * We do this for situations where we want to ensure that no more requests are made
 * over that connection and we want it closed. We ensure this by  disallowing all
 * further dispatches and transitioning to closed status on observation of a
 * [[PoisonConnectionRequest]]. We do this instead of closing the underlying resource
 * because the client stack will also attempt to cleanup the resource which would
 * expose us to multiple close calls.
 */
private[finagle] object PoisonConnection {
  val Role: Stack.Role = Stack.Role("PoisonConnection")

  def module: Stackable[ServiceFactory[Request, Result]] =
    new Stack.Module0[ServiceFactory[Request, Result]] {
      def role: Stack.Role = Role

      def description: String = "Allows the connection to be poisoned and recycled"

      def make(next: ServiceFactory[Request, Result]): ServiceFactory[Request, Result] =
        new PoisonableServiceFactory(next)
    }

  private[finagle] final class PoisonableServiceFactory(underlying: ServiceFactory[Request, Result])
      extends ServiceFactoryProxy(underlying) {

    override def apply(conn: ClientConnection): Future[Service[Request, Result]] =
      super.apply(conn).map(new PoisonableService(_))
  }

  class PoisonedConnectionException(val flags: Long)
      extends Exception
      with FailureFlags[PoisonedConnectionException]
      with NoStackTrace
      with HasLogLevel {

    // This is not a retryable failure as this exception indicates a triggered
    // session-level circuit breaker
    def this() = this(FailureFlags.Rejected | FailureFlags.NonRetryable)

    val logLevel: Level = Level.DEBUG

    protected def copyWithFlags(flags: Long): PoisonedConnectionException =
      new PoisonedConnectionException(flags)
  }

  // If we receive a poison request we fail all following requests and set our status to
  // `Closed`. We do this instead of closing the underlying connection because we don't
  // want to expose ourselves to double closes on the underlying resource since the client
  // stack will attempt to close the service once it's status transitions to `Closed`.
  final class PoisonableService(svc: Service[Request, Result])
      extends ServiceProxy[Request, Result](svc) {

    @volatile private[this] var poisoned = false

    override def apply(request: Request): Future[Result] = {
      if (poisoned || (request eq PoisonConnectionRequest)) {
        poisoned = true
        Future.exception(new PoisonedConnectionException)
      } else {
        super.apply(request)
      }
    }

    override def status: Status = if (poisoned) Status.Closed else super.status
  }
}
