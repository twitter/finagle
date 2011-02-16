package com.twitter.finagle.tracing

/** 
 * Support for tracing in finagle. The main abstraction herein is the
 * "TraceContext", which is a local that contains various metadata
 * required for distributed tracing.
 */ 

import scala.util.Random

import com.twitter.util.{Local, RichU64Long}

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

case class TraceContext(
  var traceID: TraceID,
  var transcript: Transcript
)

object TraceContext {
  private[this] val rng = new Random
  private[this] val current = new Local[TraceContext]

  def update(ctx: TraceContext) {
    current() = ctx
  }

  def apply(): TraceContext = {
    if (!current().isDefined)
      current() = newContext()

    current().get
  }

  def newContext() = {
    val traceID = TraceID(rng.nextLong(), None, Host(), VMID())
    TraceContext(traceID, NullTranscript)
  }

  def reset() {
    this() = newContext()
  }
}
