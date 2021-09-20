package com.twitter.finagle.postgresql

import com.twitter.finagle._
import com.twitter.finagle.util.AsyncLatch
import com.twitter.util._

private[finagle] object DelayedRelease {
  def module(r: Stack.Role): Stackable[ServiceFactory[Request, Response]] =
    new Stack.Module0[ServiceFactory[Request, Response]] {
      final def role: Stack.Role = r
      final def description =
        "Prevents an PostgreSql service from being closed until its response completes"
      final def make(
        next: ServiceFactory[Request, Response]
      ): ServiceFactory[Request, Response] =
        next.map(svc => new DelayedReleaseService(svc))
    }
}

/**
 * Delay the close() of streaming responses until the readers themselves have completed.
 */
private[finagle] final class DelayedReleaseService(
  service: Service[Request, Response])
    extends ServiceProxy[Request, Response](service) {

  private[this] val latch = new AsyncLatch

  override def apply(req: Request): Future[Response] = {
    latch.incr()

    service(req).transform {
      // multi-query response
      case Return(Response.SimpleQueryResponse(responses)) =>
        val observedResponses = responses.map {
          case r @ Response.ResultSet(_, rows, _) =>
            latch.incr()
            rows.onClose.ensure { latch.decr() }
            r
          case r =>
            r
        }

        observedResponses.onClose.ensure { latch.decr() }
        Future(Response.SimpleQueryResponse(observedResponses))

      // streaming response
      case r @ Return(Response.ResultSet(_, reader, _)) =>
        reader.onClose.ensure { latch.decr() }
        Future.const(r)

      // Non-streaming.
      case r =>
        latch.decr()
        Future.const(r)
    }
  }

  override final def close(deadline: Time): Future[Unit] = {
    val p = new Promise[Unit]
    latch.await { p.become(service.close(deadline)) }
    p
  }
}
