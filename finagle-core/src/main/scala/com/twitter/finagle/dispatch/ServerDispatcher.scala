package com.twitter.finagle.dispatch

import com.twitter.finagle.context.Contexts
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

  /**
   * Dispatches a request. The first argument is the request. The second
   * argument `eos` (end-of-stream promise) must be fulfilled when the request
   * is complete.
   *
   * For non-streaming requests, `eos.setDone()` should be called immediately,
   * since the entire request is present. For streaming requests,
   * `eos.setDone()` must be called at the end of stream (in HTTP, this is on
   * receipt of last chunk). Refer to the implementation in
   * [[com.twitter.finagle.http.codec.HttpServerDispatcher]].
   */
  protected def dispatch(req: Out, eos: Promise[Unit]): Future[Rep]
  protected def handle(rep: Rep): Future[Unit]

  private[this] def loop(): Future[Unit] = {
    state.set(Idle)
    trans.read() flatMap { req =>
      val p = new Promise[Rep]
      if (state.compareAndSet(Idle, p)) {
        val eos = new Promise[Unit]
        val save = Local.save()
        try trans.peerCertificate match {
          case None => p.become(dispatch(req, eos))
          case Some(cert) => Contexts.local.let(Transport.peerCertCtx, cert) {
            p.become(dispatch(req, eos))
          }
        } finally Local.restore(save)
        p map { res => (res, eos) }
      } else Eof
    } flatMap { case (rep, eos) =>
      Future.join(handle(rep), eos).unit
    } respond {
      case Return(()) if state.get ne Closed =>
        loop()

      case _ =>
        trans.close()
    }
  }

  // Clear all locals to start the loop; we want a clean slate.
  private[this] val looping = Local.letClear { loop() }

  trans.onClose ensure {
    looping.raise(cancelled)
    state.getAndSet(Closed).raise(cancelled)
  }

  /** Exposed for testing */
  protected[dispatch] def isClosing: Boolean = state.get() eq Closed

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

  protected def dispatch(req: Req, eos: Promise[Unit]) =
    service(req) ensure eos.setDone()

  protected def handle(rep: Rep) = trans.write(rep)
}
