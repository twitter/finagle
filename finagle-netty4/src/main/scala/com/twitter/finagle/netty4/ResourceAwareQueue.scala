package com.twitter.finagle.netty4

import com.twitter.concurrent.AsyncQueue
import com.twitter.logging.Logger
import com.twitter.util.{Future, Return}
import scala.util.control.NonFatal

private object ResourceAwareQueue {
  private val log = Logger.get
}

/**
 * [[AsyncQueue]] which can clean up resources on offer failure and queue shutdown.
 *
 * @param releaseFn callback for releasing resource backed messages
 *
 * @note This implementation can deterministically clean up messages backed by external
 *       resources.
 *
 *       There are three cases to consider:
 *
 *       1. A message is read from the queue.
 *
 *       2. A message is buffered to the queue and never read.
 *
 *       3. A message fails to be enqueued because the queue is failed.
 *
 *       In case #1, the only case where a satisfied read [[com.twitter.util.Future]] is produced,
 *       the onus is on the reading application to release any underlying direct
 *       buffers.
 *
 *       In the other cases this queue can release buffers as required.
 */
private[finagle] class ResourceAwareQueue[T](
    maxPendingOffers: Int,
    releaseFn: PartialFunction[T, T])
  extends AsyncQueue[T](maxPendingOffers) {
  import ResourceAwareQueue.log

  private[this] val id = identity[T]_

  override def offer(msg: T): Boolean = {
    val offerSucceeded = super.offer(msg)
    if (!offerSucceeded) {
      try {
        releaseFn.applyOrElse(msg, id)
      } catch {
        case NonFatal(exn) =>
          log.warning(exn, "caught exception in releaseFn")
      }

    }
    offerSucceeded
  }

  private[this] def throwIfUnsatisfied(a: Future[T], msg: String): T = a.poll match {
    case Some(Return(t)) => t
    case _ => throw new IllegalStateException(msg)
  }

  override def fail(exc: Throwable, discard: Boolean): Unit = {
    // we grab synchronize because we want to replace the buffered offers
    // as an atomic operation.
    this.synchronized {
      var shouldDiscard = discard
      var idx = 1
      val sz = size
      while (idx <= sz) {
        // we're guaranteed that `size` elements are
        // waiting in the `offers` queue by definition
        // of size.
        val el = throwIfUnsatisfied(poll(), s"expected $sz buffered offers, only saw $idx")

        // we know that there are no outstanding pollers
        // since size is non-zero, this makes it safe to
        // #offer on `AsyncQueue` while holding the
        // intrinsic lock.
        try {
          super.offer(releaseFn.applyOrElse(el, id))
        } catch {
          case NonFatal(exn) =>
            // guard against dropping messages from the middle of a stream
            shouldDiscard = true
            log.warning(exn, "caught exception applying releaseFn")
        }
        idx += 1
      }
      super.fail(exc, shouldDiscard)
    }
  }
}