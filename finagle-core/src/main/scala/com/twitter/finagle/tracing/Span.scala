package com.twitter.finagle.tracing

/**
 * The `Span` is the core datastructure in RPC tracing. It denotes the
 * issuance and handling of a single RPC request.
 */

import util.Random

import com.twitter.util.RichU64Long

/**
 * The span itself is an immutable datastructure. Mutations are done
 * through copying & updating span references elsewhere. If an
 * explicit root identifier is not specified, it is computed to be
 * either the parent span or this span itself.
 *
 * @param id           A 64-bit span identifier
 * @param parentId     Span identifier for the parent span
 * @param _rootId      Span identifier for the root span (aka "Trace")
 * @param transcript   The event-transcript for this span
 * @param client       The client endpoint participating in the span
 * @param server       The server endpoint participating in the span
 * @param children     A sequence of child transcripts
 */
case class Span(
  id         : Long,
  parentId   : Option[Long],
  _rootId    : Option[Long],
  transcript : Transcript,
  client     : Option[Endpoint],
  server     : Option[Endpoint],
  children   : Seq[Span])
{
  /**
   * @return the root identifier for the trace to which this span
   *         belongs
   */
  val rootId = _rootId getOrElse (parentId getOrElse id)

  /**
   * @return whether the transcript for this span is currently
   *         recording
   */
  def isRecording = transcript.isRecording
  
  /**
   * @return a pretty string for this span ID.
   */
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

  /**
   * Print this span (together with its children) to the console.
   */
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

  /**
   * Make a copy of this span with recording turned off/on.
   */
  def recording(v: Boolean) = {
    if (v && !isRecording)
      copy(transcript = new BufferingTranscript)
    else if (!v && isRecording)
      copy(transcript = NullTranscript)
    else
      this
  }

  /**
   * Merge the given spans into this one, splicing them into the span
   * tree by ID. Any spans that cannot be parented are not merged in.
   */
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

  private def splice(spans: Seq[Span]): (Span, Seq[Span]) = {
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
  private[Span] val rng = new Random

  def apply(): Span = Span(None, None, None)
  def apply(id: Option[Long], parentId: Option[Long], rootId: Option[Long]): Span =
    Span(id getOrElse rng.nextLong, parentId, rootId, NullTranscript, None, None, Seq())
}
