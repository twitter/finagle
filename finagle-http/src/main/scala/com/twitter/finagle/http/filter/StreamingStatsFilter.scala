package com.twitter.finagle.http.filter

import com.twitter.finagle._
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finagle.stats.{Counter, ExceptionStatsHandler, Stat, StatsReceiver}
import com.twitter.io.{Buf, Reader}
import com.twitter.util._
import java.util.concurrent.atomic.LongAdder

object StreamingStatsFilter {
  def module: Stackable[ServiceFactory[Request, Response]] =
    new Stack.Module2[param.Stats, param.ExceptionStatsHandler, ServiceFactory[Request, Response]] {
      val role: Stack.Role = Stack.Role("HttpStreamingStatsFilter")
      val description: String = "HTTP Streaming Stats"

      def make(
        statsParam: param.Stats,
        excStatParam: param.ExceptionStatsHandler,
        next: ServiceFactory[Request, Response]
      ): ServiceFactory[Request, Response] = {
        if (statsParam.statsReceiver.isNull) next
        else
          new StreamingStatsFilter(statsParam.statsReceiver.scope("http"), excStatParam.categorizer)
            .andThen(next)
      }
    }
}

/**
 * A filter to export statistics for HTTP streaming requests and responses.
 * A streaming request/response is an HTTP request/response with `isChunked` set to true.
 *
 * This filter is included in HTTP stack by default, all metrics defined in this filter are only
 * populated for HTTP streaming requests/responses.
 *
 * Stats:
 *    ---------------Request Stream------------
 *    - stream/request/duration_ms:
 *        A histogram of the duration of the lifetime of request streams, from the time a stream is
 *        initialized until it's closed, in milliseconds.
 *    ---------------Response Stream-----------
 *    - stream/response/duration_ms:
 *        A histogram of the duration of the lifetime of response streams, from the time a stream is
 *        initialized until it's closed, in milliseconds.
 * Counters:
 *    ---------------Request Stream------------
 *    - stream/request/closed:
 *        A counter of the number of closed request streams.
 *    - stream/request/failures
 *        A counter of the number of times any failure has been observed in the middle of a request
 *        stream.
 *    - stream/request/failures/<exception_name>:
 *        A counter of the number of times a specific exception has been thrown in the middle of a
 *        request stream.
 *    - stream/request/opened:
 *        A counter of the number of opened request streams.
 *    ---------------Response Stream-----------
 *    - stream/response/closed:
 *        A counter of the number of closed response streams.
 *    - stream/response/failures
 *        A counter of the number of times any failure has been observed in the middle of a response
 *        stream.
 *    - stream/response/failures/<exception_name>:
 *        A counter of the number of times a specific exception has been thrown in the middle of a
 *        response stream.
 *    - stream/response/opened:
 *        A counter of the number of opened response streams.
 * Gauges:
 *    ---------------Request Stream------------
 *    - stream/request/pending:
 *        A gauge of the number of pending request streams.
 *    ---------------Response Stream------------
 *    - stream/response/pending:
 *        A gauge of the number of pending response streams.
 *
 * You could derive the streaming success rate of:
 *    - the total number of streams
 *      number of successful streams divided by number of total streams
 *    - closed streams
 *      number of successful streams divided by number of closed streams
 * Here we assume a success stream as a stream terminated without an exception or a stream that has
 * not terminated yet.
 *
 * Take request stream as an example, assuming your counters are not "latched", which means that
 * their values are monotonically increasing:
 *    - Success rate of total number of streams:
 *      1 - (rated_counter(stream/request/failures)
 *           / (gauge(stream/request/pending) + rated_counter(stream/request/closed)))
 *    - Success rate of number of closed streams:
 *      1 - (rated_counter(stream/request/failures) / rated_counter(stream/request/closed))
 */
class StreamingStatsFilter(
  stats: StatsReceiver,
  exceptionStatsHandler: ExceptionStatsHandler,
  nowMillis: () => Long = Stopwatch.systemMillis)
    extends SimpleFilter[Request, Response] {

  private[this] val pendingRequestStreamsCount: LongAdder = new LongAdder()
  private[this] val pendingResponseStreamsCount: LongAdder = new LongAdder()

  // Request stream stats
  private[this] val requestStreamStat = stats.scope("stream", "request")
  private[this] val requestStreamDurationMs = requestStreamStat.stat("duration_ms")
  private[this] val openedRequestStream = requestStreamStat.counter("opened")
  private[this] val closedRequestStream = requestStreamStat.counter("closed")
  private[this] val pendingRequestStream = requestStreamStat.addGauge("pending") {
    pendingRequestStreamsCount.sum()
  }

  // Response stream stats
  private[this] val responseStreamStat = stats.scope("stream", "response")
  private[this] val responseStreamDurationMs = responseStreamStat.stat("duration_ms")
  private[this] val openedResponseStream = responseStreamStat.counter("opened")
  private[this] val closedResponseStream = responseStreamStat.counter("closed")
  private[this] val pendingResponseStream = responseStreamStat.addGauge("pending") {
    pendingResponseStreamsCount.sum()
  }

  def apply(request: Request, service: Service[Request, Response]): Future[Response] = {
    if (request.isChunked) {
      // Update stream metrics for request stream
      openedRequestStream.incr()
      pendingRequestStreamsCount.increment()
      updateClosedStream(
        request.reader,
        requestStreamStat,
        closedRequestStream,
        pendingRequestStreamsCount,
        requestStreamDurationMs)
    }
    service(request).respond {
      case Return(response) =>
        if (response.isChunked) {
          // Update stream metrics for response stream
          openedResponseStream.incr()
          pendingResponseStreamsCount.increment()
          updateClosedStream(
            response.reader,
            responseStreamStat,
            closedResponseStream,
            pendingResponseStreamsCount,
            responseStreamDurationMs)
        }
      case _ =>
    }
  }

  private def updateClosedStream(
    reader: Reader[Buf],
    statsReceiver: StatsReceiver,
    closedStreamCounter: Counter,
    pendingStreamCount: LongAdder,
    streamDurationStat: Stat
  ): Unit = {
    val streamingStart = nowMillis()
    reader.onClose.respond { closeP =>
      closedStreamCounter.incr()
      pendingStreamCount.decrement()
      val streamingEnd = nowMillis()
      streamDurationStat.add(streamingEnd - streamingStart)
      closeP match {
        case Throw(exception) =>
          exceptionStatsHandler.record(statsReceiver, exception)
        case _ =>
      }
    }
  }

}
