package com.twitter.finagle.http.exp

import com.twitter.finagle.Failure
import com.twitter.finagle.context.{Contexts, RemoteInfo}
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.CancelledRequestException
import com.twitter.util._
import java.util.concurrent.atomic.AtomicReference

private[finagle] object GenSerialServerDispatcher {
  private val Eof = Future.exception(Failure("EOF"))
  // We don't use Future.never here, because object equality is important here
  private val Idle = new NoFuture
  private val Closed = new NoFuture
}

/**
 * A generic version of
 * [[com.twitter.finagle.dispatch.SerialServerDispatcher SerialServerDispatcher]],
 * allowing the implementor to furnish custom dispatchers & handlers.
 */
private[finagle] abstract class GenSerialServerDispatcher[Req, Rep, In, Out](trans: StreamTransport[In, Out])
  extends Closable {

  def this(trans: Transport[In, Out]) = this(new IdentityStreamTransport(trans))

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
  protected def dispatch(req: Out): Future[Rep]
  protected def handle(rep: Rep): Future[Unit]

  private[this] def loop(): Future[Unit] = {
    state.set(Idle)
    trans.read().flatMap { case Multi(req, eos) =>
      val p = new Promise[Rep]
      if (state.compareAndSet(Idle, p)) {
        val save = Local.save()
        try {
          Contexts.local.let(RemoteInfo.Upstream.AddressCtx, trans.remoteAddress) {
            trans.peerCertificate match {
              case None => p.become(dispatch(req))
              case Some(cert) => Contexts.local.let(Transport.peerCertCtx, cert) {
                p.become(dispatch(req))
              }
            }
          }
        } finally Local.restore(save)
        p.map { res => (res, eos) }
      } else Eof
    }.flatMap { case (rep, eos) =>
      Future.join(handle(rep), eos).unit
    }.respond {
      case Return(()) if state.get ne Closed =>
        loop()
      case _ =>
        trans.close()
    }
  }

  // Clear all locals to start the loop; we want a clean slate.
  private[this] val looping = Local.letClear { loop() }

  trans.onClose.ensure {
    looping.raise(cancelled)
    state.getAndSet(Closed).raise(cancelled)
  }

  /** Exposed for testing */
  protected[exp] def isClosing: Boolean = state.get() eq Closed

  // Note: this is racy, but that's inherent in draining (without
  // protocol support). Presumably, half-closing a TCP connection is
  // also possible.
  def close(deadline: Time) = {
    if (state.getAndSet(Closed) eq Idle)
      trans.close(deadline)
    trans.onClose.unit
  }
}
