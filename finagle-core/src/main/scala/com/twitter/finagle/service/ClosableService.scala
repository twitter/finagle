package com.twitter.finagle.service

import com.twitter.finagle._
import com.twitter.util.{CloseOnce, Future, Time}

private[finagle] object ClosableService {
  val role = Stack.Role("ClosableService")

  /**
   * Client stack module that creates a [[ClosableService]] wrapper when services are managed
   * independently of the client stack.
   */
  def client[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module1[FactoryToService.Enabled, ServiceFactory[Req, Rep]] {
      val role = ClosableService.role
      val description = "Explicitly prevent reuse of a closed session"
      def make(
        factoryToService: FactoryToService.Enabled,
        next: ServiceFactory[Req, Rep]
      ): ServiceFactory[Req, Rep] = {
        if (factoryToService.enabled) next
        else {
          // session lifecycle is managed independently. Connections can be pooled so we must
          // make sure a closed session is not reused
          next.map(new ClosableService(_) {
            def closedException = new ServiceReturnedToPoolException
          })
        }
      }
    }
}

/**
 * A service wrapper that prevents reuse of the `underlying` service after the first call to
 * `.close`.
 */
private[service] abstract class ClosableService[Req, Rep](underlying: Service[Req, Rep])
    extends Service[Req, Rep]
    with CloseOnce {

  protected def closedException: Exception

  override def apply(req: Req): Future[Rep] = {
    if (isClosed) Future.exception(closedException)
    else underlying(req)
  }

  override protected final def closeOnce(deadline: Time): Future[Unit] = underlying.close(deadline)

  override def status: Status = {
    if (isClosed) Status.Closed
    else underlying.status
  }
}
