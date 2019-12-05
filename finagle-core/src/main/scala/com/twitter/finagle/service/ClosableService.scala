package com.twitter.finagle.service

import com.twitter.finagle._
import com.twitter.util.{Future, Time}
import java.util.concurrent.atomic.AtomicBoolean

private[finagle] object ClosableService {
  val role = Stack.Role("ClosableService")

  /**
   * Client stack module that creates a [[ClosableService]] wrapper when services are managed
   * independently of the client stack.
   */
  def client[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module1[FactoryToService.Enabled, ServiceFactory[Req, Rep]] {
      val role = ClosableService.role
      val description = "Explictly prevent reuse of a closed session"
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
    extends Service[Req, Rep] {
  private val closed = new AtomicBoolean(false)

  protected def closedException: Exception

  override def apply(req: Req): Future[Rep] = {
    if (closed.get) Future.exception(closedException)
    else underlying(req)
  }

  override def close(deadline: Time): Future[Unit] = {
    val closeUnderlying: Boolean = closed.compareAndSet(false, true)

    if (closeUnderlying) underlying.close(deadline)
    else Future.Done
  }

  override def status: Status = {
    if (closed.get) Status.Closed
    else underlying.status
  }
}
