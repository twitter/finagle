package com.twitter.finagle.zipkin.core

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.tracing.TraceId
import com.twitter.util.{Duration, Future, Local, Time, Timer}
import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable.ListBuffer

/**
 * Takes care of storing the spans in a thread safe fashion. If a span
 * is not removed from the map it will expire after the deadline is reached
 * and sent off to scribe despite being incomplete.
 *
 * Spans in the map can follow three different sequences of state transitions:
 *   (i)   live -> on hold -> logged
 *   (ii)  live -> on hold -> flushed -> logged
 *   (iii) live -> flushed -> logged
 *
 * (i) should be the most common behavior: on adding a designated trigger annotation (ClientReceive
 * or ServerSend), the hold timer is set. Its expiry causes the span to be logged immediately.
 *
 * (ii) occurs when the ttl timer expires before the hold timer. If annotations arrive after the
 * span has been logged, the span will be logged a second time with the new annotations.
 *
 * If the ttl and hold timer expire together, the span may be logged twice.
 *
 * (iii) occurs when no trigger annotation arrives before the ttl expires.
 */
private class DeadlineSpanMap(
  logSpans: Seq[Span] => Future[Unit],
  ttl: Duration,
  timer: Timer,
  hold: Duration = 500.milliseconds) {

  private[this] val spanMap = new ConcurrentHashMap[TraceId, MutableSpan](64)

  private[this] val flushTask = timer.schedule(ttl / 2) { flush(ttl.ago) }

  /**
   * Update the mutable span for the given TraceId.
   *
   * This will create a new MutableSpan if one does not exist otherwise the existing
   * span will be provided.
   *
   * If the span is on hold (almost complete), wait a short time for any more locally
   * generated annotations before removing from the map and sending to [[logSpans]].
   *
   * @param f expected to be a "quick" operation. No bitcoin mining, please.
   */
  def update(traceId: TraceId)(f: MutableSpan => Unit): Unit = {
    // there are a few "dances" here in order to avoid race conditions.
    // avoiding a removal from the map and a logSpans call is important
    // or updates written to the MutableSpan may not get logged.
    val ms: MutableSpan =
      spanMap.computeIfAbsent(
        traceId,
        new java.util.function.Function[TraceId, MutableSpan]() {
          def apply(t: TraceId): MutableSpan = new MutableSpan(traceId, Time.now)
        })

    val toFlush: Option[MutableSpan] = ms.synchronized {
      if (ms.wasFlushed) {
        // it's already been flushed. copy most of it and immediately log it.
        val copy = ms.copyForImmediateLogging()
        f(copy)
        Some(copy)
      } else {
        f(ms)
        if (ms.isOnHold) {
          // We clear the locals because we don't need them and we don't want them to
          // be dragged through the GC unnecessarily. However, since we're clearing
          // them we need to compute the `Time` at which to trigger the task beforehand.
          // If we don't we'll be missing locals used to manipulate the Time.now
          // function which is used to compute the absolute time from `hold` and this
          // can make it really tough to test.
          val when = hold.fromNow
          Local.letClear {
            timer.doAt(when) { complete(traceId, ms) }
          }
        }
        None
      }
    }

    toFlush match {
      case Some(copy) => logSpans(Seq(copy.toSpan))
      case None => ()
    }
  }

  /**
   * Flush spans created at or before the given `deadline`.
   *
   * They will be removed and have [[logSpans]] called on them.
   *
   * @return Future indicating completion.
   */
  def flush(deadline: Time): Future[Unit] = {
    val beforeDeadline = new ListBuffer[Span]()

    val iter = spanMap.entrySet.iterator()
    while (iter.hasNext) {
      val ms = iter.next().getValue
      if (ms.started <= deadline) {
        ms.synchronized {
          beforeDeadline.append(ms.toSpan)
          preventMoreUpdates(ms)
          iter.remove()
        }
      }
    }

    if (beforeDeadline.isEmpty) Future.Done
    else logSpans(beforeDeadline.toSeq)
  }

  /**
   * Flush all currently tracked spans.
   *
   * @return Future indicating completion.
   */
  def flush(): Future[Unit] =
    flush(Time.Top)

  /**
   * Remove and log the span.
   *
   * @return Future indicating done.
   */
  def complete(traceId: TraceId, ms: MutableSpan): Future[Unit] = {
    val logIt: Boolean = ms.synchronized {
      val removed = spanMap.remove(traceId, ms)
      if (removed) {
        preventMoreUpdates(ms)
        true
      } else {
        false
      }
    }
    if (logIt) logSpans(Seq(ms.toSpan))
    else Future.Done
  }

  private[this] def preventMoreUpdates(ms: MutableSpan): Unit =
    ms.flush()

}
