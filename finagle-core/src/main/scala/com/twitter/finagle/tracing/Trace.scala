package com.twitter.finagle.tracing

import scala.util.Random

import com.twitter.util.{Local, Time, TimeFormat, RichU64Long}

case class TraceID(
  var span: Long,
  var parentSpan: Option[Long],
  val host: Int,
  val vm: String)
{
  override def toString = {
    val spanHex = new RichU64Long(span).toU64HexString
    val parentSpanHex = parentSpan map (new RichU64Long(_).toU64HexString)

    val spanString = parentSpanHex match {
      case Some(parentSpanHex) => "%s<:%s".format(spanHex, parentSpanHex)
      case None => spanHex
    }

    "%s,%s".format(spanString, vm)
  }
}

object Span {
  private[Span] val timeFormat =
    new TimeFormat("yyyyMMdd.HHmmss")
}

case class Span(
  var traceID: TraceID,
  var startTime: Time,
  var endTime: Time,
  var transcript: Transcript)
{
  override def toString = {
    "%s: %s+%d".format(
      traceID,
      Span.timeFormat.format(startTime),
      (endTime - startTime).inMilliseconds)
  }

  def print() {
    transcript foreach { record =>
      val atMs = (record.timestamp - startTime).inMilliseconds
      record.message.split("\n") foreach { line =>
        println("%s %03dms: %s".format(traceID, atMs, line))
      }
    }
  }
}

object Trace {
  private[this] val rng     = new Random
  private[this] val current = new Local[Span]

  private[this] def newSpan() = {
    val traceID = TraceID(rng.nextLong(), None, Host(), VMID())
    Span(traceID, Time.now, Time.epoch, NullTranscript)
  }

  def update(ctx: Span) {
    current() = ctx
  }

  def apply(): Span = {
    if (!current().isDefined)
      current() = newSpan()

    current().get
  }

  def clear() {
    current.clear()
  }

  def startSpan() {
    this() = newSpan()
  }

  def startSpan(parentSpanID: Long) {
    startSpan()
    this().traceID.parentSpan = Some(parentSpanID)
  }

  def endSpan(): Span = {
    Trace().endTime = Time.now
    val span = Trace()
    clear()
    span
  }

  def debug(isOn: Boolean) {
    if (isOn && !Trace().transcript.isRecording)
      Trace().transcript = new BufferingTranscript(Trace().traceID)
    else if (!isOn && Trace().transcript.isRecording)
      Trace().transcript = NullTranscript
  }

  def spanID = Trace().traceID.span

  def record(message: => String) {
    Trace().transcript.record(message)
  }
}
