package com.twitter.finagle.http

import scala.collection.immutable

object HttpTracing {

  /**
   * HTTP headers used for tracing.
   *
   * See [[headers()]] for Java compatibility.
   */
  object Header {
    val TraceContext = "b3"
    val TraceId = "X-B3-TraceId"
    val SpanId = "X-B3-SpanId"
    val ParentSpanId = "X-B3-ParentSpanId"
    val Sampled = "X-B3-Sampled"
    val Flags = "X-B3-Flags"

    val All = immutable.Seq(TraceId, SpanId, ParentSpanId, Sampled, Flags)
    val Required = Seq(TraceId, SpanId)

    /** exposed for testing */
    private[http] def hasAllRequired(headers: HeaderMap): Boolean =
      headers.contains(Header.TraceId) && headers.contains(Header.SpanId)
  }

  /** Java compatibility API for [[Header]]. */
  def headers(): Header.type = Header

  /**
   * Remove any parameters from url.
   */
  private[http] def stripParameters(uri: String): String = {
    uri.indexOf('?') match {
      case -1 => uri
      case n => uri.substring(0, n)
    }
  }
}
