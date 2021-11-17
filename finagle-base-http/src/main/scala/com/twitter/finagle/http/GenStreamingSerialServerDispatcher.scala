package com.twitter.finagle.http

import com.twitter.finagle.CancelledRequestException
import com.twitter.finagle.Failure
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.context.RemoteInfo
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.util.DefaultTimer
import com.twitter.logging.Level
import com.twitter.logging.Logger
import com.twitter.util._
import java.util.concurrent.atomic.AtomicReference

private[finagle] object GenStreamingSerialServerDispatcher {

  private val logger = Logger.get()

  // Note: this is a slightly different Eof than the finagle-core version, but I don't think it matters
  private val Eof = Future.exception(Failure("EOF"))
  private val cancelled = new CancelledRequestException

  private sealed trait DispatchState
  private case object Idle extends DispatchState
  private case object Running extends DispatchState
  private case object Closing extends DispatchState
}

/**
 * A generic version of
 * [[com.twitter.finagle.dispatch.SerialServerDispatcher SerialServerDispatcher]],
 * allowing the implementor to furnish custom dispatchers & handlers.
 */
private[finagle] abstract class GenStreamingSerialServerDispatcher[Req, Rep, In, Out](
  trans: StreamTransport[In, Out])
    extends Closable {

  def this(trans: Transport[In, Out]) = this(new IdentityStreamTransport(trans))

  import GenStreamingSerialServerDispatcher._

  private[this] val state = new AtomicReference[DispatchState](Idle)

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

  /**
   * Only the dispatch loop can make state transitions to Idle and Running but close
   * operations can transition the state to Closing. If the loop finds that the state
   * has been transitioned from Idle -> Closing, it is the closer's job to close the
   * transport. If the loops finds that the state has transitioned from Running -> Closing,
   * it has been given a chance to drain the last connection and will ensure that the
   * transport is closed.
   */
  private[this] def loop(): Future[Unit] = {
    trans
      .read()
      .flatMap(dispatchAndHandleFn)
      .transform(continueLoopFn)
  }

  private[this] val handleFn: Rep => Future[Unit] = handle(_)

  // Dispatches and handles a message from the transport or closes down if necessary
  private[this] val dispatchAndHandleFn: Multi[Out] => Future[Unit] = {
    case Multi(req, eos) =>
      if (state.compareAndSet(Idle, Running)) {
        val save = Local.save()
        val dispatched =
          try {
            Contexts.local.let(
              RemoteInfo.Upstream.AddressCtx, // key 1
              trans.context.remoteAddress, // value 1
              Transport.sslSessionInfoCtx, // key 2
              trans.context.sslSessionInfo // value 2
            )(dispatch(req))
          } finally Local.restore(save)

        val handled = dispatched.flatMap(handleFn)

        // This version of `Future.join` doesn't collect the values from the Futures, but
        // since they are both Future[Unit], we know what the result is and can avoid the
        // overhead of collecting two Units just to throw them away via another flatMap.
        Future.join(handled :: eos :: Nil)
      } else {
        // must have transitioned from Idle to Closing, by someone else who is
        // responsible for closing the transport
        val st = state.get
        if (st == Closing) Eof
        else {
          // Something really bad happened. Shutdown and log as loudly as possible.
          trans.close()
          val msg = s"Dispatch loop found in illegal state: $st"
          val ex = new IllegalStateException(msg)
          logger.error(ex, msg)
          Future.exception(ex)
        }
      }
  }

  // Checks the state after a dispatch and continues or shuts down the transport if necessary
  private[this] val continueLoopFn: Try[Unit] => Future[Unit] = { res =>
    if (res.isReturn && state.compareAndSet(Running, Idle)) loop()
    else {
      // The loop has been canceled and we have been given the opportunity to drain so
      // we need to close the transport.
      // Note: We don't sequence the transport.close() Future because we don't care to wait
      // for it and also don't want to clobber the result of the loop.
      if (logger.isLoggable(Level.TRACE)) {
        if (res.isThrow) {
          logger.trace(res.throwable, s"closing $trans due to read error")
        } else {
          logger.trace(
            s"closing $trans due to status.cas failure,  state is ${state.get()}, expect Running"
          )
        }
      }

      trans.close()
      Future.const(res)
    }
  }

  // Clear all locals to start the loop; we want a clean slate.
  private[this] val looping = Local.letClear { loop() }

  trans.onClose.ensure {
    state.set(Closing)
    looping.raise(cancelled)
  }

  /** Exposed for testing */
  protected[http] def isClosing: Boolean = state.get() == Closing

  /** Exposed for testing */
  private[http] def timer: Timer = DefaultTimer

  // Note: this is racy, but that's inherent in draining (without
  // protocol support). Presumably, half-closing a TCP connection is
  // also possible.
  def close(deadline: Time): Future[Unit] = {

    // What to do next depends on the state of the dispatcher:
    // - Idle: we can close the transport immediately.
    // - Running: we need to allow time to drain. Set a timer to ensure it closes by the deadline
    // - Closing: close has already been called or the transport closed: return the trans.onClose future.
    state.getAndSet(Closing) match {
      case Idle => trans.close(deadline)
      case Running =>
        trans.onClose.by(timer, deadline).onFailure { _ =>
          trans.close(deadline) // The dispatcher took too long, ask the transport to close
        }
      case Closing => () // No action required.
    }

    trans.onClose.unit
  }
}
