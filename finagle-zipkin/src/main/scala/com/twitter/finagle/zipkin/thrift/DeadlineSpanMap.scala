package com.twitter.finagle.zipkin.thrift

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
 */
private class DeadlineSpanMap(
    logSpans: Seq[Span] => Future[Unit],
    ttl: Duration,
    statsReceiver: StatsReceiver,
    timer: Timer)
{

  private[this] val spanMap = new ConcurrentHashMap[TraceId, MutableSpan](64)

  private[this] val timerTask = timer.schedule(ttl / 2) { flush(ttl.ago) }

  /**
   * Update the mutable span.
   *
   * This will create a new MutableSpan if one does not exist otherwise the existing
   * span will be provided.
   *
   * If the span is deemed complete it will be removed from the map and sent to `logSpans`
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

    if (span.isComplete) {
      spanMap.remove(traceId, span)
      logSpans(Seq(span.toSpan))
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

  def addAnnotation(ann: ZipkinAnnotation): MutableSpan = synchronized {
    if (!_isComplete && (
      ann.value.equals(Constants.CLIENT_RECV) ||
      ann.value.equals(Constants.SERVER_SEND) ||
      ann.value.equals(TimeoutFilter.TimeoutAnnotation)
    )) _isComplete = true

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

  def isComplete: Boolean = synchronized { _isComplete }
}
