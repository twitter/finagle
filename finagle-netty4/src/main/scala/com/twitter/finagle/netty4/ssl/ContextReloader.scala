package com.twitter.finagle.netty4.ssl

import com.twitter.conversions.DurationOps._
import com.twitter.logging.Logger
import com.twitter.util._
import io.netty.handler.ssl.SslContext
import java.util.concurrent.atomic.AtomicReference

private object ContextReloader {
  val log = Logger.get
}

/**
 * Class to support asynchronous refreshes of [[SslContext]].
 *
 * Reloads are best-effort and resilient to non-fatal exceptions after the initial evaluation.
 * Out-of-date values may be returned if `ctxFac` does not return within `reloadPeriod`.
 */
final private class ContextReloader(
  ctxFac: => SslContext,
  timer: Timer,
  reloadPeriod: Duration = 1.minute,
  pool: FuturePool = FuturePool.unboundedPool)
    extends Closable {
  import ContextReloader.log

  // this evaluation of `ctxFac` could entail synchronous i/o happening on
  // class construction.
  private[this] val ctx: AtomicReference[SslContext] = new AtomicReference(ctxFac)

  // all writes happen in timer thread
  @volatile private[this] var lastFacUpdate: Future[Unit] = Future.Done

  private[this] val respondFn: Try[SslContext] => Unit = {
    case Return(rebuiltContext) =>
      ctx.set(rebuiltContext)
    case Throw(t) =>
      log.warning(t, "failed to reload SslContext")
  }

  private[this] val reloadTask = timer.schedule(reloadPeriod) {
    if (!lastFacUpdate.isDefined) { // the last factory call hasn't completed
      log.debug(s"failed to reload SslContext within $reloadPeriod")
    } else {
      // bounce ctxFac through a pool since we expect sync i/o
      val fFac = pool(ctxFac)
      fFac.respond(respondFn)
      lastFacUpdate = fFac.unit
    }
  }

  /**
   * The most recent [[SslContext]]. There are no other guarantees about recency.
   */
  def sslContext: SslContext = ctx.get()

  def close(deadline: Time): Future[Unit] = {
    reloadTask.cancel()
    Future.Done
  }
}
