package com.twitter.finagle.zipkin.core

import com.twitter.conversions.time._
import com.twitter.finagle.service.TimeoutFilter
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.thrift.thrift.Constants
import com.twitter.finagle.tracing.TraceId
import com.twitter.util.{Duration, Future, Time, Timer}
import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable.ArrayBuffer

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
  statsReceiver: StatsReceiver,
  timer: Timer,
  hold: Duration = 500.milliseconds) {

  private[this] val spanMap = new ConcurrentHashMap[TraceId, MutableSpan](64)

  private[this] val timerTask = timer.schedule(ttl / 2) { flush(ttl.ago) }

  /**
   * Update the mutable span.
   *
   * This will create a new MutableSpan if one does not exist otherwise the existing
   * span will be provided.
   *
   * If the span is on hold (almost complete), wait a short time for any more locally
   * generated annotations before removing from the map and sending to `logSpans`.
   */
  def update(traceId: TraceId)(f: MutableSpan => Unit): Unit = {
    val span: MutableSpan = {
      val span = spanMap.get(traceId)
      if (span != null) {
        span
      } else {
        val newSpan = new MutableSpan(traceId, Time.now)
        val prev = spanMap.putIfAbsent(traceId, newSpan)
        if (prev == null) newSpan else prev
      }
    }

    f(span)

    if (span.isOnHold) {
      timer.doLater(hold) { complete(traceId, span) }
    }
  }

  /**
   * Flush spans created earlier than `now`
   *
   * @return Future indicating completion.
   */
  def flush(deadline: Time): Future[Unit] = {
    val ss = new ArrayBuffer[Span](spanMap.size)

    val iter = spanMap.entrySet.iterator
    while (iter.hasNext) {
      val kv = iter.next()
      val span = kv.getValue
      if (span.started <= deadline) {
        spanMap.remove(kv.getKey, span)
        span.addAnnotation(ZipkinAnnotation(deadline, "finagle.flush", span.endpoint))
        ss.append(span.toSpan)
      }
    }

    if (ss.isEmpty) Future.Done
    else logSpans(ss)
  }

  /**
   * Remove and log the span.
   * @param span
   * @return Future indicating done.
   */
  def complete(traceId: TraceId, span: MutableSpan): Future[Unit] = {
    val removed = spanMap.remove(traceId, span)
    if (removed)
      logSpans(Seq(span.toSpan))
    Future.Done
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
  private[this] var _name: Option[String] = None
  private[this] var _service: Option[String] = None
  private[this] var _endpoint: Endpoint = Endpoint.Unknown
  private[this] var _onHold: Boolean = false

  private[this] val annotations = ArrayBuffer.empty[ZipkinAnnotation]
  private[this] val binaryAnnotations = ArrayBuffer.empty[BinaryAnnotation]

  def endpoint: Endpoint = synchronized { _endpoint }

  def setName(n: String): MutableSpan = synchronized {
    _name = Some(n)
    this
  }

  def setServiceName(n: String): MutableSpan = synchronized {
    _service = Some(n)
    this
  }

  /**
   * Add an annotation to the map.
   *
   * The special annotations ClientRecv and ServerSend, at client or server respectively,
   * are taken as a hint that the span will complete soon. In this case, we don't set the span
   * complete right away because additional annotations may still arrive. We want to try and avoid
   * annotations that arrive after the rest of the span has been logged because they are likely to
   * get dropped at the collector.
   */
  def addAnnotation(ann: ZipkinAnnotation): MutableSpan = synchronized {
    if (ann.value.equals(Constants.CLIENT_RECV) ||
      ann.value.equals(Constants.SERVER_SEND) ||
      ann.value.equals(TimeoutFilter.TimeoutAnnotation)) {
      _onHold = true
    }

    annotations.append(ann)
    this
  }

  def addBinaryAnnotation(ann: BinaryAnnotation): MutableSpan = synchronized {
    binaryAnnotations.append(ann)
    this
  }

  def setEndpoint(ep: Endpoint): MutableSpan = synchronized {
    _endpoint = ep
    var idx = 0
    while (idx < annotations.size) {
      val a = annotations(idx)
      if (a.endpoint == Endpoint.Unknown) annotations(idx) = a.copy(endpoint = ep)
      idx += 1
    }
    this
  }

  def toSpan: Span = synchronized {
    Span(traceId, _service, _name, annotations, binaryAnnotations, _endpoint)
  }

  def isOnHold: Boolean = synchronized { _onHold }
}
