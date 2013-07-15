package com.twitter.finagle.zipkin.thrift

import collection.mutable.HashMap
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.tracing.TraceId
import com.twitter.util.{Time, Timer, Duration, Future, TimerTask}

/**
 * Takes care of storing the spans in a thread safe fashion. If a span
 * is not removed from the map it will expire after the deadline is reached
 * and sent off to scribe despite being incomplete.
 */
class DeadlineSpanMap(tracer: RawZipkinTracer,
                      deadline: Duration,
                      statsReceiver: StatsReceiver,
                      timer: Timer) {

  private[this] val spanMap = HashMap[TraceId, (Span, TimerTask)]()

  /**
   * Update the span in the map. If none exists create a new span. Synchronized.
   */
  def update(traceId: TraceId)(f: Span => Span): Span = synchronized {
    val (span, task) = spanMap.get(traceId) match {
      case Some((s, t)) => (f(s), t)
      case None =>
        // no span found, let's create a new one
        val span = f(Span(traceId))

        // if this new span isn't triggered by a natural end we
        // send off what we have anyway
        val task = timer.schedule(deadline.fromNow) {
          remove(traceId) foreach {
            statsReceiver.scope("log_span").counter("unfinished").incr()
            tracer.logSpan(_)
          }
        }

        (span, task)
    }

    spanMap.put(traceId, (span, task))
    span
  }

  /**
   * Remove this span from the map and cancel its task. Synchronized.
   */
  def remove(traceId: TraceId): Option[Span] = synchronized {
    for ((span,task) <- spanMap.remove(traceId)) yield {
      task.cancel()
      span
    }
  }

  /**
   * Flush all currently tracked spans.
   *
   * @return Future indicating completion.
   */
  def flush(): Future[Unit] = synchronized {
    val logged = for (id <- spanMap.keys.toSeq; raw <- remove(id)) yield {
      val span = raw.copy(annotations =
        ZipkinAnnotation(Time.now, "finagle.flush", raw.endpoint, None) +: raw.annotations)
      tracer.logSpan(span)
    }
    Future.join(logged)
  }
}
