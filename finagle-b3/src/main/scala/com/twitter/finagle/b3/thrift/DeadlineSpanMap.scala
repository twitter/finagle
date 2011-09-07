package com.twitter.finagle.b3.thrift

import com.twitter.util.Duration
import collection.mutable.HashMap
import com.twitter.finagle.tracing.TraceId
import com.twitter.finagle.util.Timer
import com.twitter.finagle.stats.StatsReceiver

/**
 * Takes care of storing the spans in a thread safe fashion. If a span
 * is not removed from the map it will expire after the deadline is reached
 * and sent off to B3 despite being incomplete.
 */
class DeadlineSpanMap(tracer: BigBrotherBirdTracer, deadline: Duration, statsReceiver: StatsReceiver) {

  private[this] val spanMap = HashMap[TraceId, Span]()
  private[this] val timer = Timer.default

  /**
   * Update the span in the map. If none exists create a new span. Synchronized.
   */
  def update(traceId: TraceId)(f: Span => Span): Span = synchronized {
    val span = spanMap.get(traceId) match {
      case Some(s) => f(s)
      case None =>
        // no span found, let's create a new one
        val span = f(Span(traceId))

        // if this new span isn't triggered by a natural end we
        // send off what we have anyway
        timer.schedule(deadline.fromNow) {
          statsReceiver.scope("log_span").counter("unfinished").incr()
          remove(traceId) foreach { tracer.logSpan(_) }
        }

        span
    }

    spanMap.put(traceId, span)
    span
  }

  /**
   * Remove this span from the map. Synchronized.
   */
  def remove(traceId: TraceId): Option[Span] = synchronized {
    spanMap.remove(traceId)
  }

}