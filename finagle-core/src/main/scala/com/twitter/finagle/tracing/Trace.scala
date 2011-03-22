package com.twitter.finagle.tracing

import scala.util.Random

import com.twitter.util.{Local, Time, TimeFormat, RichU64Long}

object Trace {
  private[this] case class Ref(var span: Span)

  // This is the currently active span to which we are recording.
  private[this] val current = new Local[Ref]

  private def update(span: Span) {
    ref().span = span
  }

  private def ref(): Ref = {
    if (!current().isDefined)
      current() = Ref(Span())

    current().get
  }

  def apply(): Span = ref().span

  def id(): Long = this().rootId

  def clear() {
    current.clear()
  }

  def startSpan(id: Option[Long], parentId: Option[Long], rootId: Option[Long]) {
    clear()
    this() = Span(id, parentId, rootId)
  }

  def startSpan() {
    startSpan(None, None, None)
  }

  def endSpan(): Span = {
    val span = this()
    clear()
    span
  }

  def addChild(): Span = {
    val span = Span(None, Some(this().id), this()._rootId).recording(isRecording)
    this() = this().copy(children = this().children ++ Seq(span))
    span
  }

  def debug(isOn: Boolean) {
    this() = this().recording(isOn)
  }

  def isRecording = this().isRecording

  def record(annotation: Annotation) {
    this().transcript.record(annotation)
  }

  def record(message: => String) {
    record(Annotation.Message(message))
  }

  def merge(spans: Seq[Span]) {
    this() = this().merge(spans)
  }
}
