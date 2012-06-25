package com.twitter.finagle.dispatch

import com.twitter.finagle.transport.Transport
import com.twitter.finagle.{Service, NoStacktrace}
import com.twitter.util.{Future, Return}
import java.util.concurrent.atomic.AtomicReference

/**
 * A {{ServerDispatcher}} dispatches requests onto services from a
 * source such as a {{Transport}}.
 */
trait ServerDispatcher {
  /**
   * Accept no more requests.
   */
  def drain()
}

object ServerDispatcher {
  val nil: ServerDispatcher = new ServerDispatcher {
    def drain() {}
  }
}

object SerialServerDispatcher {
  private val Eof = Future.exception(new Exception("EOF") with NoStacktrace)
  private val Idle = Future.never
  private val Draining = Future.never
  private val Closed = Future.never
}

/**
 * Dispatch requests from transport one at a time, queueing
 * concurrent requests.
 *
 * Transport errors are considered fatal; the service will be
 * released after any error.
 */
class SerialServerDispatcher[Req, Rep](trans: Transport[Rep, Req], service: Service[Req, Rep])
  extends ServerDispatcher
{
  import SerialServerDispatcher._

  private[this] val state = new AtomicReference[Future[_]](Idle)

  trans.onClose ensure {
    state.getAndSet(Closed).cancel()
    service.release()
  }

  private[this] def loop(): Unit = {
    state.set(Idle)
    trans.read() flatMap { req =>
      val f = service(req)
      if (state.compareAndSet(Idle, f)) f else {
        f.cancel()
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
  def drain() {
    if (state.getAndSet(Draining) eq Idle)
      trans.close()
  }
}
