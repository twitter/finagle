package com.twitter.finagle.zipkin.core

import com.twitter.finagle.service.TimeoutFilter
import com.twitter.finagle.thrift.thrift.Constants
import com.twitter.finagle.tracing.{Trace, TraceId}
import com.twitter.util.Time
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable.ArrayBuffer

/**
 * Note that with regards to thread-safety, [[DeadlineSpanMap]] effectively
 * "owns" this class and also participates in synchronizing on the instance itself.
 *
 * @see [[DeadlineSpanMap]]
 */
private final class MutableSpan(val traceId: TraceId, val started: Time) {
  private[this] val flushed = new AtomicBoolean(false)

  // thread-safety for all mutable state is handled by synchronizing on `this`.
  private[this] var _name: Option[String] = None
  private[this] var _service: Option[String] = None
  private[this] var _endpoint: Endpoint = Endpoint.Unknown
  private[this] var _onHold: Boolean = false

  private[this] val annotations = ArrayBuffer.empty[ZipkinAnnotation]
  private[this] val binaryAnnotations = ArrayBuffer.empty[BinaryAnnotation]

  /**
   * Mark this as having been [[DeadlineSpanMap.flush()]]-ed.
   * No further updates will be included when logged.
   *
   * @see [[copyForImmediateLogging()]]
   */
  def flush(): Unit = flushed.set(true)

  /**
   * Whether or not this has been flushed.
   */
  def wasFlushed: Boolean = flushed.get

  /**
   * Used for races during [[DeadlineSpanMap.update]]. Creates a copy
   * of most of the fields, but does not include annotations, binary annotations,
   * or onHold.
   */
  def copyForImmediateLogging(): MutableSpan = synchronized {
    val ms = new MutableSpan(traceId, started)
    ms.setEndpoint(endpoint)
    _service match {
      case Some(s) => ms.setServiceName(s)
      case _ => // no-op
    }
    _name match {
      case Some(n) => ms.setName(n)
      case _ => // no-op
    }
    // do not copy onHold since we are about to log it.
    // do not copy annotations or binaryAnnotations since they would already be logged.
    ms
  }

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
      ann.value.equals(TimeoutFilter.TimeoutAnnotation) ||
      ann.value.equals(Trace.LocalEndAnnotation)) {
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
    Span(traceId, _service, _name, annotations.toList, binaryAnnotations.toList, _endpoint, started)
  }

  def isOnHold: Boolean = synchronized { _onHold }
}
