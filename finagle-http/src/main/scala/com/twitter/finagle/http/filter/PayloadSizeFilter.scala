package com.twitter.finagle.http.filter

import com.twitter.finagle.Stack.Module1
import com.twitter.finagle.http.{Chunk, Request, RequestProxy, Response, ResponseProxy}
import com.twitter.finagle.param.Stats
import com.twitter.finagle.{Service, ServiceFactory, SimpleFilter, Stack, param}
import com.twitter.finagle.stats.{Stat, StatsReceiver, Verbosity}
import com.twitter.finagle.tracing.{Trace, Tracing}
import com.twitter.io.{Reader, StreamTermination}
import com.twitter.util.{Future, Return, Throw, Try}
import java.util.concurrent.atomic.AtomicLong

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

  private val reqKey: String = "request_payload_bytes"
  private val repKey: String = "response_payload_bytes"
  private val chunkKey: String = "chunk_payload_bytes"

  private final class RecordingChunkReader(
    parent: Reader[Chunk],
    trace: Tracing,
    stat: Stat,
    traceKey: String)
      extends Reader[Chunk] { self =>

    private[this] val bytesObserved = new AtomicLong()

    private[this] val observeRead: Try[Option[Chunk]] => Unit = {
      case Return(Some(chunk)) =>
        val len = chunk.content.length
        bytesObserved.addAndGet(len)
        stat.add(len)

      case Return(None) =>
        if (trace.isActivelyTracing) trace.recordBinary(traceKey, bytesObserved.get)

      case Throw(_) => // nop
    }

    def discard(): Unit = parent.discard()

    def onClose: Future[StreamTermination] = parent.onClose

    def read(): Future[Option[Chunk]] = parent.read().respond(observeRead)
  }
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
  import PayloadSizeFilter._

  private[this] val streamReqTraceKey: String = s"${prefix}stream/request/${chunkKey}"
  private[this] val streamRepTraceKey: String = s"${prefix}stream/response/${chunkKey}"
  private[this] val reqTraceKey: String = prefix + reqKey
  private[this] val repTraceKey: String = prefix + repKey

  private[this] val requestBytes: Stat = statsReceiver.stat(Verbosity.Debug, reqKey)
  private[this] val responseBytes: Stat = statsReceiver.stat(Verbosity.Debug, repKey)

  private[this] val streamRequestBytes: Stat =
    statsReceiver.scope("stream").scope("request").stat(Verbosity.Debug, chunkKey)
  private[this] val streamResponseBytes: Stat =
    statsReceiver.scope("stream").scope("response").stat(Verbosity.Debug, chunkKey)

  private[this] def handleResponse(trace: Tracing): Response => Response = { rep =>
    if (rep.isChunked) {
      new ResponseProxy {
        override def response: Response = rep
        override val chunkReader: Reader[Chunk] =
          new RecordingChunkReader(super.chunkReader, trace, streamResponseBytes, streamRepTraceKey)
      }
    } else {
      recordBufferedSize(rep.content.length, trace, responseBytes, repTraceKey)
      rep
    }
  }

  private[this] def recordBufferedSize(
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
        override val chunkReader: Reader[Chunk] =
          new RecordingChunkReader(super.chunkReader, trace, streamRequestBytes, streamReqTraceKey)
      }
    } else {
      recordBufferedSize(req.content.length, trace, requestBytes, reqTraceKey)
      req
    }
    service(request).map(handleResponse(trace))
  }
}
