package com.twitter.finagle.zipkin.thrift

/**
 * The `Span` is the core datastructure in RPC tracing. It denotes the
 * issuance and handling of a single RPC request.
 */

import java.nio.ByteBuffer

import scala.collection.Map
import collection.JavaConversions._
import com.twitter.finagle.thrift.thrift
import com.twitter.finagle.tracing.TraceId


/**
 * The span itself is an immutable datastructure. Mutations are done
 * through copying & updating span references elsewhere.
 *
 * @param traceId      Contains the Trace id (same for whole trace), Span id (same for just one
 * rpc call) and parent id (links to the parent span in this trace, if None this is the root span)
 * @param _serviceName  The name of the service handling the RPC
 * @param _name         The name of the RPC method
 * @param annotations  A sequence of annotations made in this span
 * @param bAnnotations Key-Value annotations, used to attach non timestamped data
 * @param _endpoint    This is the endpoint the span was created on.
 */
case class Span(
  traceId      : TraceId,
  _serviceName : Option[String],
  _name        : Option[String],
  annotations  : Seq[ZipkinAnnotation],
  bAnnotations : Seq[BinaryAnnotation],
  _endpoint    : Option[Endpoint])
{
  val serviceName = _serviceName getOrElse "Unknown"
  val name = _name getOrElse "Unknown"
  val endpoint = _endpoint getOrElse Endpoint.Unknown

  /**
   * @return a pretty string for this span ID.
   */
  def idString = {
    val spanString = traceId.spanId.toString
    val parentSpanString = traceId._parentId map (_.toString)

    parentSpanString match {
      case Some(parentSpanString) => "%s<:%s".format(spanString, parentSpanString)
      case None => spanString
    }
  }

  def toThrift: thrift.Span = {
    val span = new thrift.Span

    span.setId(traceId.spanId.toLong)
    traceId._parentId foreach { parentId => span.setParent_id(parentId.toLong) }
    span.setTrace_id(traceId.traceId.toLong)
    span.setName(name)
    span.setDebug(traceId.flags.isDebug)

    annotations map ( _.toThrift ) foreach { a =>
      if (a.isSetHost) a.getHost().setService_name(serviceName)
      span.addToAnnotations(a)
    }

    bAnnotations map ( _.toThrift ) foreach { a =>
      if (a.isSetHost) a.getHost().setService_name(serviceName)
      span.addToBinary_annotations(a)
    }
    span
  }

}

object Span {
  def apply(traceId: TraceId): Span = Span(traceId, None, None, Seq(), Seq(), None)
}
