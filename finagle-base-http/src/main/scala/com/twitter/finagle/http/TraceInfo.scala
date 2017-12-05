package com.twitter.finagle.http

import com.twitter.finagle.tracing.{Flags, SpanId, Trace, TraceId, TraceId128}
import java.lang.{Boolean => JBoolean, Long => JLong}

private object TraceInfo {
  import com.twitter.finagle.http.HttpTracing._

  private[this] def removeAllHeaders(headers: HeaderMap): Unit = {
    val iter = Header.All.iterator
    while (iter.hasNext) headers -= iter.next()
  }

  def letTraceIdFromRequestHeaders[R](request: Request)(f: => R): R = {
    val id =
      if (Header.hasAllRequired(request.headerMap)) {
        val spanId = SpanId.fromString(request.headerMap(Header.SpanId))

        spanId match {
          case None => None
          case Some(sid) =>
            val trace128Bit = TraceId128(request.headerMap(Header.TraceId))

            val parentSpanId =
              if (request.headerMap.contains(Header.ParentSpanId))
                SpanId.fromString(request.headerMap(Header.ParentSpanId))
              else
                None

            // we use the getOrNull api to shave an allocation, since this
            // may be on the hot path
            val maybeSampled = request.headerMap.getOrNull(Header.Sampled)
            val sampled = if (maybeSampled == null) {
              None
            } else {
              Some("1".equals(maybeSampled) || JBoolean.valueOf(maybeSampled))
            }

            val flags = getFlags(request)
            Some(TraceId(trace128Bit.low, parentSpanId, sid, sampled, flags, trace128Bit.high))
        }
      } else if (request.headerMap.contains(Header.Flags)) {
        // even if there are no id headers we want to get the debug flag
        // this is to allow developers to just set the debug flag to ensure their
        // trace is collected
        Some(Trace.nextId.copy(flags = getFlags(request)))
      } else {
        Some(Trace.nextId)
      }

    // remove so the header is not visible to users
    removeAllHeaders(request.headerMap)

    id match {
      case Some(tid) =>
        Trace.letId(tid) {
          traceRpc(request)
          f
        }
      case None =>
        traceRpc(request)
        f
    }
  }

  def setClientRequestHeaders(request: Request): Unit = {
    removeAllHeaders(request.headerMap)

    val traceId = Trace.id
    val traceIdString = if (traceId.traceIdHigh.isEmpty) {
      traceId.traceId.toString
    } else {
      traceId.traceIdHigh.get.toString + traceId.traceId.toString
    }
    request.headerMap.add(Header.TraceId, traceIdString)
    request.headerMap.add(Header.SpanId, traceId.spanId.toString)
    // no parent id set means this is the root span
    traceId._parentId match {
      case Some(id) =>
        request.headerMap.add(Header.ParentSpanId, id.toString)
      case None => ()
    }
    // three states of sampled, yes, no or none (let the server decide)
    traceId.sampled match {
      case Some(sampled) =>
        request.headerMap.add(Header.Sampled, sampled.toString)
      case None => ()
    }
    if (traceId.flags.toLong != 0L) {
      request.headerMap.add(Header.Flags, JLong.toString(traceId.flags.toLong))
    }
    traceRpc(request)
  }

  def traceRpc(request: Request): Unit = {
    if (Trace.isActivelyTracing) {
      Trace.recordRpc(request.method.toString)
      Trace.recordBinary("http.uri", stripParameters(request.uri))
    }
  }

  /**
   * Safely extract the flags from the header, if they exist. Otherwise return empty flag.
   */
  def getFlags(request: Request): Flags = {
    if (!request.headerMap.contains(Header.Flags)) {
      Flags()
    } else {
      try Flags(JLong.parseLong(request.headerMap(Header.Flags)))
      catch {
        case _: NumberFormatException => Flags()
      }
    }
  }
}
