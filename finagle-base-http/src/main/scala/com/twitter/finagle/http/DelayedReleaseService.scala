package com.twitter.finagle.http

import com.twitter.finagle._
import com.twitter.finagle.util.AsyncLatch
import com.twitter.util.{Future, Promise, Return, Time}

private[finagle] object DelayedRelease {
  def module(r: Stack.Role): Stackable[ServiceFactory[Request, Response]] =
    new Stack.Module1[FactoryToService.Enabled, ServiceFactory[Request, Response]] {
      final def role: Stack.Role = r
      final def description =
        "Prevents an HTTP service from being closed until its response completes"
      final def make(
        _enabled: FactoryToService.Enabled,
        next: ServiceFactory[Request, Response]
      ): ServiceFactory[Request, Response] =
        if (_enabled.enabled) next.map(new DelayedReleaseService(_))
        else next
    }
}

/**
 * Delay release of the connection until all chunks have been received.
 */
private[finagle] class DelayedReleaseService[-Req <: Request](service: Service[Req, Response])
    extends ServiceProxy[Req, Response](service) {

  private val latch = new AsyncLatch

  override def apply(req: Req): Future[Response] = {
    latch.incr()

    service(req).transform {
      // Streaming Req and Rep.
      case r @ Return(rep) if req.isChunked && rep.isChunked =>
        req.reader.onClose.ensure {
          rep.reader.onClose.ensure { latch.decr() }
        }
        Future.const(r)

      // Streaming Rep.
      case r @ Return(rep) if rep.isChunked =>
        rep.reader.onClose.ensure { latch.decr() }
        Future.const(r)

      // Streaming Req.
      case r if req.isChunked =>
        req.reader.onClose.ensure { latch.decr() }
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
