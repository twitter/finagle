package com.twitter.finagle.zipkin.thrift

import collection.mutable.{ArrayBuffer, HashMap}
import com.twitter.finagle.service.TimeoutFilter
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.thrift.thrift.Constants
import com.twitter.finagle.tracing.TraceId
import com.twitter.util.{Time, Timer, Duration, Future, TimerTask}

/**
 * Takes care of storing the spans in a thread safe fashion. If a span
 * is not removed from the map it will expire after the deadline is reached
 * and sent off to scribe despite being incomplete.
 */
private class DeadlineSpanMap(
  logSpans: Seq[Span] => Future[Unit],
  ttl: Duration,
  statsReceiver: StatsReceiver,
  timer: Timer
) {

  private[this] val spanMap = HashMap[TraceId, MutableSpan]()
  private[this] val unfinishedCounter = statsReceiver.scope("log_span").counter("unfinished")

  private[this] val timerTask = timer.schedule(ttl / 2) { flush(ttl.ago) }

  /**
   * Synchronously update the mutable span.
   *
   * This will create a new MutableSpan if one does not exist otherwise the existing
   * span will be provided.
   *
   * If the span is deemed complete it will be removed from the map and sent to `logSpans`
   */
  def update(traceId: TraceId)(f: MutableSpan => Unit): Unit = synchronized {
    val span = spanMap.get(traceId) match {
      case Some(span) =>
        f(span)
        span

      case None =>
        val span = new MutableSpan(traceId, Time.now)
        spanMap.put(traceId, span)
        f(span)
        span
    }

    if (span.isComplete) {
      remove(traceId)
      logSpans(Seq(span.toSpan))
    }
  }

  /**
   * Synchronize and remove the span from the map.
   */
  def remove(traceId: TraceId): Option[MutableSpan] = synchronized {
    spanMap.remove(traceId)
  }

  /**
   * Flush spans created earlier than `now`
   *
   * @return Future indicating completion.
   */
  def flush(deadline: Time): Future[Unit] = {
    val spans = new ArrayBuffer[Span](spanMap.size)

    synchronized {
      spanMap.keys.toList foreach { traceId =>
        spanMap.get(traceId) match {
          case Some(span) if span.started <= deadline =>
            remove(traceId)
            span.addAnnotation(ZipkinAnnotation(deadline, "finagle.flush", span.endpoint, None))
            spans.append(span.toSpan)
          case _ => ()
        }
      }
    }

    if (spans.isEmpty) Future.Done
    else logSpans(spans.toSeq)
  }

  /**
   * Flush all currently tracked spans.
   *
   * @return Future indicating completion.
   */
  def flush(): Future[Unit] =
    flush(Time.Top)
}

private final class MutableSpan(val traceId: TraceId, val started: Time) {
  private[this] var _isComplete: Boolean = false
  private[this] var _name: Option[String] = None
  private[this] var _service: Option[String] = None
  private[this] var _endpoint: Endpoint = Endpoint.Unknown

  private[this] var annotations = ArrayBuffer.empty[ZipkinAnnotation]
  private[this] val binaryAnnotations = ArrayBuffer.empty[BinaryAnnotation]

  def endpoint: Endpoint = _endpoint

  def setName(n: String): MutableSpan = {
    _name = Some(n)
    this
  }

  def setServiceName(n: String): MutableSpan = {
    _service = Some(n)
    this
  }

  def addAnnotation(ann: ZipkinAnnotation): MutableSpan = {
    if (!_isComplete && (
      ann.value.equals(Constants.CLIENT_RECV) ||
      ann.value.equals(Constants.SERVER_SEND) ||
      ann.value.equals(TimeoutFilter.TimeoutAnnotation)
    )) _isComplete = true

    annotations.append(ann)
    this
  }

  def addBinaryAnnotation(ann: BinaryAnnotation): MutableSpan = {
    binaryAnnotations.append(ann)
    this
  }

  def setEndpoint(ep: Endpoint): MutableSpan = {
    _endpoint = ep
    var idx = 0
    while (idx < annotations.size) {
      val a = annotations(idx)
      if (a.endpoint == Endpoint.Unknown) annotations(idx) = a.copy(endpoint = ep)
      idx += 1
    }
    this
  }

  def toSpan: Span =
    Span(traceId, _service, _name, annotations, binaryAnnotations, _endpoint)

  def isComplete: Boolean = _isComplete
}
