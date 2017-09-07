package com.twitter.finagle.http

import com.twitter.finagle
import com.twitter.finagle._
import com.twitter.finagle.client.StackClient
import com.twitter.finagle.util.AsyncLatch
import com.twitter.io.Reader
import com.twitter.util.{Future, Promise, Return, Throw, Time}
import java.util.concurrent.atomic.AtomicBoolean

private[finagle] object DelayedRelease {
  val role = StackClient.Role.prepFactory
  val description = "Prevents an HTTP service from being closed until its response completes"
  val module: Stackable[ServiceFactory[Request, Response]] =
    new Stack.Module1[FactoryToService.Enabled, ServiceFactory[Request, Response]] {
      val role = DelayedRelease.role
      val description = DelayedRelease.description
      def make(_enabled: FactoryToService.Enabled, next: ServiceFactory[Request, Response]) =
        if (_enabled.enabled) next.map(new DelayedReleaseService(_))
        else next
    }
}

/**
 * Delay release of the connection until all chunks have been received.
 */
private[finagle] class DelayedReleaseService[-Req <: Request](
  service: Service[Req, Response]
) extends ServiceProxy[Req, Response](service) {

  protected[this] val latch = new AsyncLatch

  private[this] def proxy(in: Response): Response = {
    val released = new AtomicBoolean(false)
    def done() {
      if (released.compareAndSet(false, true)) {
        latch.decr()
      }
    }

    new finagle.http.Response.Proxy {

      def response: Response = in

      override def reader: Reader = new Reader {
        def read(n: Int) = in.reader.read(n) respond {
          case Return(None) => done()
          case Throw(_) => done()
          case _ =>
        }

        def discard() = {
          // Note: Discarding the underlying reader terminates the session and
          // marks the service as unavailable. It's important that we discard
          // before releasing the service (by invoking `done`), to ensure that
          // the service wrapper in the pool will create a new service instead
          // of reusing this one whose transport is closing.
          in.reader.discard()
          done()
        }
      }
    }
  }

  override def apply(request: Req): Future[Response] = {
    latch.incr()
    service(request) transform {
      case Return(r) if r.isChunked =>
        Future.value(proxy(r))
      case t =>
        latch.decr()
        Future.const(t)
    }
  }

  override final def close(deadline: Time): Future[Unit] = {
    val p = new Promise[Unit]
    latch.await { p.become(service.close(deadline)) }
    p
  }

}
