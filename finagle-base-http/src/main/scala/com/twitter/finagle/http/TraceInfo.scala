package com.twitter.finagle.http

import com.twitter.finagle.tracing.{Flags, SpanId, Trace, TraceId, TraceId128}
import java.lang.{Boolean => JBoolean, Long => JLong}

private object TraceInfo {
  import com.twitter.finagle.http.HttpTracing._

  private[this] def removeAllHeaders(headers: HeaderMap): Unit = {
    val iter = Header.All.iterator
    while (iter.hasNext) headers -= iter.next()
  }

  def convertB3Trace(request: Request) =
    if (request.headerMap.contains(Header.TraceContext)) {
      def handleSampled(headers: HeaderMap, value: String): Unit =
        value match {
          case "0" => headers.set(Header.Flags, "0")
          case "d" => headers.set(Header.Flags, "1")
          case "1" => headers.set(Header.Sampled, "1")
          case _ => ()
        }
      def handleTraceAndSpanIds(headers: HeaderMap, a: String, b: String): Unit = {
        headers.set(Header.TraceId, a)
        headers.set(Header.SpanId, b)
      }

      val _ = request.headerMap.get(Header.TraceContext).map(_.split("-").toList) match {
        case Some(a) =>
          val headers = request.headerMap
          a.size match {
            case 0 =>
              // bogus
              ()
            case 1 =>
              // either debug flag or sampled
              handleSampled(headers, a(0))
            case 2 =>
              // this is required to be traceId, spanId
              handleTraceAndSpanIds(headers, a(0), a(1))
            case 3 =>
              handleTraceAndSpanIds(headers, a(0), a(1))
              handleSampled(headers, a(2))
            case 4 =>
              handleTraceAndSpanIds(headers, a(0), a(1))
              handleSampled(headers, a(2))
              headers.set(Header.ParentSpanId, a(3))
          }
        case None =>
          ()
      }
      request.headerMap -= "b3"
    }

  def letTraceIdFromRequestHeaders[R](request: Request)(f: => R): R = {
    // rather than rewrite all this to handle reading and writing the
    // new b3 trace header format this code sets up the request to
    // allow the existing code to consume, but not produce, b3 header
    // traces.
    convertB3Trace(request)

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
          f
        }
      case None =>
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
    request.headerMap.addUnsafe(Header.TraceId, traceIdString)
    request.headerMap.addUnsafe(Header.SpanId, traceId.spanId.toString)
    // no parent id set means this is the root span
    traceId._parentId match {
      case Some(id) =>
        request.headerMap.addUnsafe(Header.ParentSpanId, id.toString)
      case None => ()
    }
    // three states of sampled, yes, no or none (let the server decide)
    traceId.sampled match {
      case Some(sampled) =>
        request.headerMap.addUnsafe(Header.Sampled, sampled.toString)
      case None => ()
    }
    if (traceId.flags.toLong != 0L) {
      request.headerMap.addUnsafe(Header.Flags, JLong.toString(traceId.flags.toLong))
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
