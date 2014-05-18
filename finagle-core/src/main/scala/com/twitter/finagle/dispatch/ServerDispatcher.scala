package com.twitter.finagle.dispatch

import com.twitter.finagle.transport.Transport
import com.twitter.finagle.{Service, NoStacktrace, CancelledRequestException}
import com.twitter.util._
import java.util.concurrent.atomic.AtomicReference

object GenSerialServerDispatcher {
  private val Eof = Future.exception(new Exception("EOF") with NoStacktrace)
  // We don't use Future.never here, because object equality is important here
  private val Idle = new NoFuture
  private val Closed = new NoFuture
}

/**
 * A generic version of
 * [[com.twitter.finagle.dispatch.SerialServerDispatcher SerialServerDispatcher]],
 * allowing the implementor to furnish custom dispatchers & handlers.
 */
abstract class GenSerialServerDispatcher[Req, Rep, In, Out](trans: Transport[In, Out])
    extends Closable {

  import GenSerialServerDispatcher._

  private[this] val state = new AtomicReference[Future[_]](Idle)
  private[this] val cancelled = new CancelledRequestException

  protected def dispatch(req: Out): Future[Rep]
  protected def handle(rep: Rep): Future[Unit]

  private[this] def loop(): Future[Unit] = {
    state.set(Idle)
    trans.read() flatMap { req =>
      val p = new Promise[Rep]
      if (state.compareAndSet(Idle, p)) {
        val save = Local.save()
        try p.become(dispatch(req))
        finally Local.restore(save)
        p
      } else Eof
    } flatMap { rep =>
      handle(rep)
    } respond {
      case Return(()) if state.get ne Closed =>
        loop()

      case _ =>
        trans.close()
    }
  }

  private[this] val looping = loop()

  trans.onClose ensure {
    looping.raise(cancelled)
    state.getAndSet(Closed).raise(cancelled)
  }

  // Note: this is racy, but that's inherent in draining (without
  // protocol support). Presumably, half-closing TCP connection is
  // also possible.
  def close(deadline: Time) = {
    if (state.getAndSet(Closed) eq Idle)
      trans.close(deadline)
    trans.onClose.unit
  }
}


/**
 * Dispatch requests from transport one at a time, queueing
 * concurrent requests.
 *
 * Transport errors are considered fatal; the service will be
 * released after any error.
 */
class SerialServerDispatcher[Req, Rep](
    trans: Transport[Rep, Req],
    service: Service[Req, Rep])
    extends GenSerialServerDispatcher[Req, Rep, Rep, Req](trans) {

  trans.onClose ensure {
    service.close()
  }

  protected def dispatch(req: Req) = service(req)
  protected def handle(rep: Rep) = trans.write(rep)
}
