package com.twitter.finagle.dispatch

import com.twitter.finagle.transport.Transport
import com.twitter.finagle.{Service, NoStacktrace, CancelledRequestException}
import com.twitter.util._
import java.util.concurrent.atomic.AtomicReference

object SerialServerDispatcher {
  private val Eof = Future.exception(new Exception("EOF") with NoStacktrace)
  // We don't use Future.never here, because object equality is important here
  private val Idle = new NoFuture
  private val Draining = new NoFuture
  private val Closed = new NoFuture
}

/**
 * Dispatch requests from transport one at a time, queueing
 * concurrent requests.
 *
 * Transport errors are considered fatal; the service will be
 * released after any error.
 */
class SerialServerDispatcher[Req, Rep](trans: Transport[Rep, Req], service: Service[Req, Rep])
  extends Closable
{
  import SerialServerDispatcher._

  private[this] val state = new AtomicReference[Future[_]](Idle)

  trans.onClose ensure {
    state.getAndSet(Closed).raise(new CancelledRequestException)
    service.close()
  }

  private[this] def loop(): Unit = {
    state.set(Idle)
    trans.read() flatMap { req =>
      val f = service(req)
      if (state.compareAndSet(Idle, f)) f else {
        f.raise(new CancelledRequestException)
        Eof
      }
    } flatMap { rep =>
      trans.write(rep)
    } respond {
      case Return(()) if state.get ne Draining =>
        loop()

      case _ =>
        trans.close()
    }
  }

  loop()

  // Note: this is racy, but that's inherent in draining (without
  // protocol support). Presumably, half-closing TCP connection is
  // also possible.
  def close(deadline: Time) = {
    if (state.getAndSet(Draining) eq Idle)
      trans.close(deadline)
    trans.onClose map(_ => ())
  }
}
