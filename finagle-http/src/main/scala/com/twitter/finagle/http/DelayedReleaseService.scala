package com.twitter.finagle.http

import com.twitter.finagle.util.AsyncLatch
import com.twitter.finagle.{Service, ServiceProxy}
import com.twitter.io.{Buf, Reader, Writer}
import com.twitter.util.{Future, Promise, Return, Throw, Time}
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Delay release of the connection until all chunks have been received.
 */
private[finagle] class DelayedReleaseService[-Req <: Request](
  service: Service[Req, Response]
) extends ServiceProxy[Req, Response](service) {

  protected[this] val counter = new AsyncLatch

  private[this] def proxy(in: Response) = {
    val released = new AtomicBoolean(false)
    def done() {
      if (released.compareAndSet(false, true)) {
        counter.decr()
      }
    }

    Response(
      in.httpResponse,
      new Reader {
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
    )
  }

  override def apply(request: Req): Future[Response] = {
    counter.incr()
    service(request) transform {
      case Return(r) if r.isChunked =>
        Future.value(proxy(r))
      case t =>
        counter.decr()
        Future.const(t)
    }
  }

  override final def close(deadline: Time): Future[Unit] = {
    val p = new Promise[Unit]
    counter.await { p.become(service.close(deadline)) }
    p
  }

}
