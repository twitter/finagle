package com.twitter.finagle.zipkin.core

/**
 * The `Span` is the core datastructure in RPC tracing. It denotes the
 * issuance and handling of a single RPC request.
 */
import com.twitter.finagle.thrift.thrift
import com.twitter.finagle.tracing.TraceId
import com.twitter.util.Time

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
 * @param endpoint     This is the local endpoint the span was created on.
 * @param created      Optional span creation time.
 */
case class Span(
  traceId: TraceId,
  _serviceName: Option[String],
  _name: Option[String],
  annotations: Seq[ZipkinAnnotation],
  bAnnotations: Seq[BinaryAnnotation],
  endpoint: Endpoint,
  created: Time) {

  def this(
    traceId: TraceId,
    _serviceName: Option[String],
    _name: Option[String],
    annotations: Seq[ZipkinAnnotation],
    bAnnotations: Seq[BinaryAnnotation],
    endpoint: Endpoint
  ) =
    this(
      traceId,
      _serviceName,
      _name,
      annotations,
      bAnnotations,
      endpoint,
      created = Time.nowNanoPrecision)

  // If necessary, we compute the timestamp of when the span was created
  // which we serialize and send to the collector.
  private[this] lazy val timestamp: Time = {
    // If we have annotations which were created before
    // the span, we synthesize the span creation time
    // to match since it's illogical for the span to be
    // created before annotations.
    (created +: annotations.map(_.timestamp)).min
  }

  val serviceName = _serviceName getOrElse "Unknown"
  val name = _name getOrElse "Unknown"

  /**
   * @return a pretty string for this span ID.
   */
  def idString: String = {
    val spanString = traceId.spanId.toString
    val parentSpanString = traceId._parentId map (_.toString)

    parentSpanString match {
      case Some(parentSpanString) => "%s<:%s".format(spanString, parentSpanString)
      case None => spanString
    }
  }

  def toThrift: thrift.Span = {
    val span = new thrift.Span

    span.setTimestamp(timestamp.inMicroseconds)

    span.setId(traceId.spanId.toLong)
    traceId._parentId match {
      case Some(id) => span.setParent_id(id.toLong)
      case None => ()
    }
    span.setTrace_id(traceId.traceId.toLong)
    span.setName(name)
    span.setDebug(traceId.flags.isDebug)

    // fill in the host/service data for all the annotations
    annotations foreach { ann =>
      val a = ann.toThrift
      val ep = if (a.isSetHost) a.getHost() else endpoint.boundEndpoint.toThrift
      ep.setService_name(serviceName)
      a.setHost(ep)
      span.addToAnnotations(a)
    }

    bAnnotations foreach { ann =>
      val a = ann.toThrift
      val ep = if (a.isSetHost) a.getHost() else endpoint.boundEndpoint.toThrift
      ep.setService_name(serviceName)
      a.setHost(ep)
      span.addToBinary_annotations(a)
    }
    span
  }

}

object Span {
  def apply(traceId: TraceId): Span =
    Span(traceId, None, None, Nil, Nil, Endpoint.Unknown, Time.nowNanoPrecision)

  def apply(
    traceId: TraceId,
    _serviceName: Option[String],
    _name: Option[String],
    annotations: Seq[ZipkinAnnotation],
    bAnnotations: Seq[BinaryAnnotation],
    endpoint: Endpoint
  ): Span =
    Span(traceId, _serviceName, _name, annotations, bAnnotations, endpoint, Time.nowNanoPrecision)
}
