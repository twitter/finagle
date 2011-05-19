package com.twitter.finagle.tracing

/**
 * The `Span` is the core datastructure in RPC tracing. It denotes the
 * issuance and handling of a single RPC request.
 */

import util.Random

import com.twitter.util.{RichU64Long, Time}
import scala.collection.Map
import java.nio.ByteBuffer
import java.net.SocketAddress
import java.net.InetSocketAddress

/**
 * Endpoints describe a TCP endpoint that terminates RPC
 * communication.
 */

case class Endpoint(ipv4: Int, port: Short)
object Endpoint {
  val Unknown = Endpoint(0, 0)

  /**
   * @return If possible, convert from a SocketAddress object to an Endpoint.
   * If not, return Unknown Endpoint.
   */
  def fromSocketAddress(socketAddress: SocketAddress): Endpoint = {
    socketAddress match {
      case inet: InetSocketAddress => {
        val addr = ByteBuffer.wrap(inet.getAddress.getAddress).getInt
        Endpoint(addr, inet.getPort.toShort)
      }
      case _ => Endpoint.Unknown
    }
  }
}

sealed trait Event
object Event {
  case class ClientSend()             extends Event
  case class ClientRecv()             extends Event
  case class ServerSend()             extends Event
  case class ServerRecv()             extends Event
  case class Message(content: String) extends Event
}

case class Annotation(
  timestamp: Time,
  event:     Event,
  endpoint:  Endpoint
)

/**
 * The span itself is an immutable datastructure. Mutations are done
 * through copying & updating span references elsewhere. If an
 * explicit root identifier is not specified, it is computed to be
 * either the parent span or this span itself.
 *
 * @param _traceId     Span identifier for the root span (aka "Trace")
 * @param serviceName  The name of the service handling the RPC
 * @param name         The name of the RPC method
 * @param id           Identifier for this span
 * @param parentId     Span identifier for the parent span. None if this is the root span.
 * @param annotations  A sequence of annotations made in this span
 * @param bAnnotations Key-Value annotations, used to attach non timestamped data
 * @param endpoint     Endpoint for this service, used to populate annotations
 * @param children     A sequence of child transcripts
 */
case class Span(
  _traceId    : Option[SpanId],
  _serviceName : Option[String],
  _name        : Option[String],
  id          : SpanId,
  parentId    : Option[SpanId],
  annotations : Seq[Annotation],
  bAnnotations: Map[String, ByteBuffer],
  endpoint    : Option[Endpoint],
  children    : Seq[Span])
{
  /**
   * @return the root identifier for the trace to which this span
   *         belongs
   */
  val traceId = _traceId getOrElse (parentId getOrElse id)
  val serviceName = _serviceName getOrElse "Unknown"
  val name = _name getOrElse "Unknown"

  /**
   * @return a pretty string for this span ID.
   */
  def idString = {
    val spanString = id.toString
    val parentSpanString = parentId map (_.toString)

    parentSpanString match {
      case Some(parentSpanString) => "%s<:%s".format(spanString, parentSpanString)
      case None => spanString
    }
  }

  override def toString = {
    "<Span " +
    idString +
    (if (children.isEmpty) "" else " " + (children mkString ",")) +
    ">"
  }

  /**
   * Print this span (together with its children) to the console.
   */
  def print(): Unit = print(0)
  def print(indent: Int) {
    annotations foreach { annotation =>
      val atMs = annotation.timestamp.inMilliseconds
      val lines: Seq[String] = annotation.event match {
        case Event.Message(text) => text.split("\n")
        case event => Seq(event.toString)
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

    val newAnnotations = spansToMerge flatMap { _.annotations }
    val newBAnnotations = spansToMerge flatMap { _.bAnnotations }
    val newSpan = copy(
      children = splicedChildren ++ newChildren,
      annotations = annotations ++ newAnnotations,
      bAnnotations = bAnnotations ++ newBAnnotations
    )

    (newSpan, nextSpans)
  }
}

object Span {
  private[Span] val rng = new Random

  def apply(): Span = Span(None, None, None, None, None, None)
  def apply(
    traceId: Option[SpanId], id: Option[SpanId], parentId: Option[SpanId],
    serviceName: Option[String], name: Option[String], endpoint: Option[Endpoint]
  ): Span = Span(
    _traceId = traceId,
    _serviceName = serviceName,
    _name = name,
    id = id getOrElse SpanId(rng.nextLong),
    parentId = parentId,
    annotations = Seq(),
    bAnnotations = Map(),
    endpoint = endpoint,
    children = Seq()
  )
}
