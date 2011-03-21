package com.twitter.finagle.tracing

/** 
 * Support for tracing in finagle. The main abstraction herein is the
 * "Trace", which is a local that contains various metadata required
 * for distributed tracing as well as references to the local traced
 * events. We mimic Dapper in many ways, including borrowing its
 * nomenclature.
 *
 * “Dapper, a Large-Scale Distributed Systems Tracing Infrastructure”,
 * Benjamin H. Sigelman, Luiz André Barroso, Mike Burrows, Pat
 * Stephenson, Manoj Plakal, Donald Beaver, Saul Jaspan, Chandan
 * Shanbhag, 2010.
 *
 *   http://research.google.com/pubs/archive/36356.pdf
 */ 

import scala.util.Random

import com.twitter.util.{Local, Time, TimeFormat, RichU64Long}

/**
 * Identifies a span, including parent & root span information.
 */
case class SpanId(
  id:        Long,
  parentId:  Option[Long],
  _rootId:   Option[Long],
  host:      Int,
  vm:        String)
{
  val rootId = _rootId getOrElse (parentId getOrElse id)

  override def toString = {
    val spanHex = new RichU64Long(id).toU64HexString
    val parentSpanHex = parentId map (new RichU64Long(_).toU64HexString)

    val spanString = parentSpanHex match {
      case Some(parentSpanHex) => "%s<:%s".format(spanHex, parentSpanHex)
      case None => spanHex
    }

    "%s,%s".format(spanString, vm)
  }
}

object SpanId {
  private[this] val rng = new Random
  def apply(): SpanId = SpanId(rng.nextLong(), None, None, Host(), VMID())
}

case class Span(
  var spanId: SpanId,
  var startTime: Time,
  var endTime: Time,
  var transcript: Transcript
) {
  override def toString = {
    "%s: %s+%d".format(
      spanId,
      Span.timeFormat.format(startTime),
      (endTime - startTime).inMilliseconds)
  }

  def print() {
    transcript foreach { record =>
      val atMs = (record.timestamp - startTime).inMilliseconds
      val lines: Seq[String] = record.annotation match {
        case Annotation.Message(text) => text.split("\n")
        case annotation => Seq(annotation.toString)
      }

      lines foreach { line =>
        println("%s %03dms: %s".format(spanId, atMs, line))
      }
    }
  }
}

object Span {
  private[Span] val timeFormat =
    new TimeFormat("yyyyMMdd.HHmmss")

  def apply(): Span = Span(SpanId())
  def apply(id: SpanId): Span = Span(id, Time.now, Time.epoch, NullTranscript)  
}

object Trace {
  private[this] val current = new Local[Span]

  private[this] def newSpan() = {
  }

  def update(ctx: Span) {
    current() = ctx
  }

  def apply(): Span = {
    if (!current().isDefined)
      current() = Span()

    current().get
  }

  def clear() {
    current.clear()
  }

  def startSpan(spanId: SpanId) {
    this() = Span(spanId)
  }

  def startSpan() {
    startSpan(SpanId())
  }

  def endSpan(): Span = {
    Trace().endTime = Time.now
    val span = Trace()
    clear()
    span
  }

  def debug(isOn: Boolean) {
    if (isOn && !Trace().transcript.isRecording)
      Trace().transcript = new BufferingTranscript(Trace().spanId)
    else if (!isOn && Trace().transcript.isRecording)
      Trace().transcript = NullTranscript
  }

  def spanID = Trace().spanId.id

  def record(message: => String) {
    Trace().transcript.record(Annotation.Message(message))
  }
}
