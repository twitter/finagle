package com.twitter.finagle.http.filter

import com.twitter.finagle.Stack.Module1
import com.twitter.finagle.http.{Chunk, Request, RequestProxy, Response, ResponseProxy}
import com.twitter.finagle.param.Stats
import com.twitter.finagle.{Service, ServiceFactory, SimpleFilter, Stack, param}
import com.twitter.finagle.stats.{Stat, StatsReceiver, Verbosity}
import com.twitter.finagle.tracing.{Trace, Tracing}
import com.twitter.io.Reader
import com.twitter.util.Future

private[finagle] object PayloadSizeFilter {
  val Role: Stack.Role = Stack.Role("HttpPayloadSize")
  val Description: String = "Reports Http request/response payload sizes"

  private[finagle] def module(prefix: String) =
    new Module1[param.Stats, ServiceFactory[Request, Response]] {
      def make(
        stats: Stats,
        next: ServiceFactory[Request, Response]
      ): ServiceFactory[Request, Response] = {
        new PayloadSizeFilter(stats.statsReceiver, prefix).andThen(next)
      }

      def role: Stack.Role = PayloadSizeFilter.Role
      def description: String = PayloadSizeFilter.Description
    }

  val clientTraceKeyPrefix: String = "clnt/"
  val serverTraceKeyPrefix: String = "srv/"
}

/**
 * A filter that exports two histograms to a given [[StatsReceiver]].
 *
 * For non-streaming messages (messages are not chunked), the two histograms are:
 * 1. "request_payload_bytes" - a distribution of request payload sizes in bytes
 * 2. "response_payload_bytes" - a distribution of response payload sizes in bytes
 *
 * For streaming messages (isChunked equals true), the two histograms are:
 * 1. "stream/request/chunk_payload_bytes" - a distribution of request's chunk payload sizes in bytes
 * 2. "stream/response/chunk_payload_bytes" - a distribution of response's chunk payload sizes in bytes
 *
 * The sizes are also traced using the binary annotations metrics name above with a "clnt/" prefix
 * on the client side, and "srv/" prefix on the server side.
 */
private[finagle] class PayloadSizeFilter(statsReceiver: StatsReceiver, prefix: String)
    extends SimpleFilter[Request, Response] {

  private[this] val reqKey = "request_payload_bytes"
  private[this] val repKey = "response_payload_bytes"
  private[this] val chunkKey = "chunk_payload_bytes"

  private[this] val streamReqTraceKey = s"${prefix}stream/request/${chunkKey}"
  private[this] val streamRepTraceKey = s"${prefix}stream/response/${chunkKey}"
  private[this] val reqTraceKey = prefix + reqKey
  private[this] val repTraceKey = prefix + repKey

  private[this] val requestBytes = statsReceiver.stat(Verbosity.Debug, reqKey)
  private[this] val responseBytes = statsReceiver.stat(Verbosity.Debug, repKey)

  private[this] val streamRequestBytes =
    statsReceiver.scope("stream").scope("request").stat(Verbosity.Debug, chunkKey)
  private[this] val streamResponseBytes =
    statsReceiver.scope("stream").scope("response").stat(Verbosity.Debug, chunkKey)

  private[this] def handleResponse(trace: Tracing): Response => Response = { rep =>
    if (rep.isChunked) {
      new ResponseProxy {
        override def response: Response = rep
        override def chunkReader: Reader[Chunk] =
          super.chunkReader
            .map(onRead(recordRepSize, trace, streamResponseBytes, streamRepTraceKey))
      }
    } else {
      recordRepSize(rep.content.length, trace, responseBytes, repTraceKey)
      rep
    }
  }

  private[this] def onRead(
    record: (Int, Tracing, Stat, String) => Unit,
    trace: Tracing,
    stat: Stat,
    traceKey: String
  ): Chunk => Chunk = { chunk =>
    record(chunk.content.length, trace, stat, traceKey)
    chunk
  }

  private[this] def recordReqSize(
    size: Int,
    trace: Tracing,
    reqStat: Stat,
    traceKey: String
  ): Unit = {
    reqStat.add(size.toFloat)
    if (trace.isActivelyTracing) trace.recordBinary(traceKey, size)
  }

  private[this] def recordRepSize(
    size: Int,
    trace: Tracing,
    repStat: Stat,
    traceKey: String
  ): Unit = {
    repStat.add(size.toFloat)
    if (trace.isActivelyTracing) trace.recordBinary(traceKey, size)
  }

  def apply(req: Request, service: Service[Request, Response]): Future[Response] = {
    val trace = Trace()
    val request = if (req.isChunked) {
      new RequestProxy {
        override def request: Request = req
        override def chunkReader: Reader[Chunk] =
          super.chunkReader.map(onRead(recordReqSize, trace, streamRequestBytes, streamReqTraceKey))
      }
    } else {
      recordReqSize(req.content.length, trace, requestBytes, reqTraceKey)
      req
    }
    service(request).map(handleResponse(trace))
  }
}
