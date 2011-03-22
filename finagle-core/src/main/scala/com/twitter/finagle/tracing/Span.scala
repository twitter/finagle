package com.twitter.finagle.tracing

import util.Random

import com.twitter.util.{TimeFormat, Time, RichU64Long}

case class Span(
  id         : Long,
  parentId   : Option[Long],
  _rootId    : Option[Long],
  transcript : Transcript,
  children   : Seq[Span])
{
  val rootId = _rootId getOrElse (parentId getOrElse id)

  def isRecording = transcript.isRecording
  
  def idString = {
    val spanHex = new RichU64Long(id).toU64HexString
    val parentSpanHex = parentId map (new RichU64Long(_).toU64HexString)

    parentSpanHex match {
      case Some(parentSpanHex) => "%s<:%s".format(spanHex, parentSpanHex)
      case None => spanHex
    }
  }

  override def toString = {
    "<Span %s>".format(idString)
  }

  def print(): Unit = print(0)
  def print(indent: Int) {
    transcript foreach { record =>
      val atMs = record.timestamp.inMilliseconds
      val lines: Seq[String] = record.annotation match {
        case Annotation.Message(text) => text.split("\n")
        case annotation => Seq(annotation.toString)
      }

      lines foreach { line =>
        println("%s%s %03dms: %s".format(" " * indent, idString, atMs, line))
      }
    }

    // Inline children at the appropriate split-off points (ie. where
    // we see the ClientSend, etc. events)
    children foreach { _.print(indent + 2) }
  }

  def recording(v: Boolean) = {
    if (v && !isRecording)
      copy(transcript = new BufferingTranscript)
    else if (!v && isRecording)
      copy(transcript = NullTranscript)
    else
      this
  }

  def merge(spans: Seq[Span]): Span = {
    val parented = spans map { parent =>
      val children = spans filter { child =>
        child.parentId match {
          case Some(id) if id == parent.id => true
          case _ => false
        }
      }

      parent.copy(children = Set() ++ (parent.children ++ children) toSeq)
    }

    val (newSpan, _) = splice(parented)
    newSpan
  }

  def splice(spans: Seq[Span]): (Span, Seq[Span]) = {
    // First split out spans that we need to merge
    val (spansToMerge, otherSpans) = spans partition { _.id == id }

    // Then splice our children, returning spans yet unmatched.
    val (splicedChildren, unmatchedSpans) =
      children.foldLeft((List[Span](), otherSpans)) {
        case ((children, spans), child) =>
          val (nextChild, nextSpans) = child.splice(spans)
          (nextChild :: children, nextSpans)
      }

    // Now split out new children, and any unmatched spans.
    val (newChildren, nextSpans) =
      unmatchedSpans partition { _.parentId map { _ == id } getOrElse false }

    spansToMerge foreach { span => transcript.recordAll(span.transcript.iterator) }
    val newSpan = copy(children = splicedChildren ++ newChildren)

    (newSpan, nextSpans)
  }
}

object Span {
  private[Span] val timeFormat =
    new TimeFormat("yyyyMMdd.HHmmss")
  private[Span] val rng = new Random

  def apply(): Span = Span(None, None, None)
  def apply(id: Option[Long], parentId: Option[Long], rootId: Option[Long]): Span =
    Span(id getOrElse rng.nextLong, parentId, rootId, NullTranscript, Seq())
}
