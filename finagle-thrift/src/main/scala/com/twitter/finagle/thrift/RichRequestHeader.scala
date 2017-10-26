package com.twitter.finagle.thrift

import com.twitter.finagle.{Dentry, Dtab, Path, NameTree}
import com.twitter.finagle.tracing.{Flags, SpanId, TraceId}
import com.twitter.finagle.thrift.thrift.RequestHeader

/**
 * Convenience class with some common RequestHeader accessors
 */
private[finagle] class RichRequestHeader(val header: RequestHeader) extends AnyVal {
  def clientId: Option[ClientId] =
    if (header.isSetClient_id && header.getClient_id.isSetName) {
      Some(ClientId(header.getClient_id.getName))
    } else {
      None
    }

  def dest: Path = if (header.isSetDest) Path.read(header.dest) else Path.empty

  def dtab: Dtab = {
    var dtab = Dtab.empty
    if (header.getDelegationsSize() > 0) {
      val ds = header.getDelegationsIterator()
      while (ds.hasNext()) {
        val d = ds.next()
        if (d.isSetSrc && d.isSetDst) {
          val src = Dentry.Prefix.read(d.getSrc)
          val dst = NameTree.read(d.getDst)
          dtab += Dentry(src, dst)
        }
      }
    }
    dtab
  }

  def traceId: TraceId =
    TraceId(
      Some(SpanId(header.getTrace_id)),
      if (header.isSetParent_span_id) Some(SpanId(header.getParent_span_id)) else None,
      SpanId(header.getSpan_id),
      if (header.isSetSampled) Some(header.isSampled) else None,
      if (header.isSetFlags) Flags(header.getFlags) else Flags(),
      if (header.isSetTrace_id_high) Some(SpanId(header.getTrace_id_high)) else None
    )
}
