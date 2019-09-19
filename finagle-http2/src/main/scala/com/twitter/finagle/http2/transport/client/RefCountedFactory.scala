package com.twitter.finagle.http2.transport.client

import com.twitter.finagle._
import com.twitter.logging.Logger
import com.twitter.util.{Future, Time}
import java.util.concurrent.atomic.AtomicInteger
import scala.annotation.tailrec

/**
 * A `ServiceFactory` backed by a single `Service`.
 *
 * Each service that is checked will only decrement the reference
 * count a single time regardless of how many times close is called.
 * This is useful for multiplexed protocols such as H2 which are best
 * represented as a `Service` which can dispatch requests concurrently.
 * In that scenario we want to allow many requests to checkout the same
 * underlying `Service` but we need to make sure that we account for all
 * the outstanding checkouts when cleaning up the resource.
 */
private class RefCountedFactory[Req, Rep](underlying: Service[Req, Rep])
    extends ServiceFactory[Req, Rep] {

  private[this] val counter = new RefCountedFactory.Counter
  private[this] var factoryClosed = false

  private[this] final class ServiceWrapper extends ServiceProxy(underlying) {
    private[this] var wrapperClosed = false

    override def close(deadline: Time): Future[Unit] = {
      // We want close-only-once semantics.
      val shouldClose = synchronized {
        if (wrapperClosed) false
        else {
          wrapperClosed = true
          true
        }
      }

      if (shouldClose && counter.checkin()) {
        underlying.close(deadline)
      } else {
        Future.Done
      }
    }
  }

  /**
   * The current availability [[Status]] of this ServiceFactory
   */
  override def status: Status =
    if (counter.isClosing) Status.Closed
    else underlying.status

  final def apply(conn: ClientConnection): Future[Service[Req, Rep]] = {
    if (counter.checkout()) Future.value(new ServiceWrapper)
    else {
      // we're closed
      Future.exception(Failure("Returned unavailable service", FailureFlags.Retryable))
    }
  }

  // Only the first invocation of this will have an effect.
  def close(deadline: Time): Future[Unit] = {
    val shouldClose = synchronized {
      if (factoryClosed) false
      else {
        factoryClosed = true
        true
      }
    }

    if (shouldClose && counter.close()) underlying.close(deadline)
    else Future.Done
  }
}

private object RefCountedFactory {

  // Manages a reference count and includes close semantics.
  private final class Counter extends AtomicInteger(1) {

    // We have some checks for illegal states, and in those circumstances we are
    // want to be conservative and close the session so we don't leak it. We also
    // log an error so that we can fix that bug.
    private[this] def illegalState(msg: String): Boolean = {
      val ex = new IllegalStateException(msg)
      // Since we hope this will never happen we get a logger instance only if we need it.
      Logger()
        .error(
          ex,
          "Illegal state detected. Please report this error " +
            "to the Finagle maintainers.")
      true
    }

    // We start life with a single reference count for the necessary `close` call.
    // When we close, we either decrement to 0 which signals closed,
    // or we flip negative which signals draining.

    def isClosing: Boolean = get <= 0

    @tailrec
    def close(): Boolean = get match {
      case i if i <= 0 => illegalState("Duplicate close detected")
      case 1 =>
        if (compareAndSet(1, 0)) true
        else close()

      case i => // i >= 2, decrement by 1 and flip negative.
        if (compareAndSet(i, 1 - i)) false
        else close()
    }

    @tailrec
    def checkout(): Boolean = get match {
      case i if i <= 0 => // draining or closed
        false
      case i =>
        if (compareAndSet(i, i + 1)) true
        else checkout()
    }

    @tailrec
    def checkin(): Boolean = get match {
      case 0 | 1 => illegalState("Duplicate checkin detected")
      case -1 => // last drainer
        if (compareAndSet(-1, 0)) true
        else illegalState("Race to be the final checkin detected")

      case i if i < -1 => // draining
        if (compareAndSet(i, i + 1)) false
        else checkin()

      case i => // open
        if (compareAndSet(i, i - 1)) false
        else checkin()
    }
  }
}
