package com.twitter.finagle.zipkin.thrift

import com.google.common.io.BaseEncoding
import com.twitter.conversions.storage._
import com.twitter.conversions.time._
import com.twitter.finagle.{Service, SimpleFilter, Thrift}
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.stats.{ClientStatsReceiver, NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.thrift.Protocols
import com.twitter.finagle.tracing._
import com.twitter.finagle.util.DefaultTimer
import com.twitter.finagle.zipkin.core.{RawZipkinTracer, Span, TracerCache}
import com.twitter.finagle.zipkin.thriftscala.{LogEntry, ResultCode, Scribe}
import com.twitter.scrooge.TReusableMemoryTransport
import com.twitter.util._
import java.io.CharArrayWriter
import java.net.InetSocketAddress
import java.util.concurrent.ArrayBlockingQueue
import org.apache.thrift.TByteArrayOutputStream
import scala.collection.mutable.ArrayBuffer

object ScribeRawZipkinTracer {
  val tracerCache = new TracerCache[ScribeRawZipkinTracer]

  private[this] def newClient(
    scribeHost: String,
    scribePort: Int,
    name: String
  ): Scribe.FutureIface = {
    val transport = ClientBuilder()
      .stack(Thrift.client)
      .name(name)
      .hosts(new InetSocketAddress(scribeHost, scribePort))
      .reportTo(ClientStatsReceiver)
      .hostConnectionLimit(5)
      // using an arbitrary, but bounded number of waiters to avoid memory leaks
      .hostConnectionMaxWaiters(250)
      // somewhat arbitrary, but bounded timeouts
      .timeout(1.second)
      .daemon(true)
      .build()

    new Scribe.FinagledClient(
      new TracelessFilter andThen transport,
      Protocols.binaryFactory())
  }

  /**
   * Creates a [[com.twitter.finagle.tracing.Tracer]] that sends traces to scribe with the specified
   * scribeCategory.
   *
   * @param scribeHost Host to send trace data to
   * @param scribePort Port to send trace data to
   * @param scribeCategory Category under which the trace data should be scribed
   * @param statsReceiver Where to log information about tracing success/failures
   * @param clientName Name of the scribe finagle client
   */
  def apply(
    scribeHost: String,
    scribePort: Int,
    scribeCategory: String,
    statsReceiver: StatsReceiver,
    timer: Timer,
    clientName: String
  ): ScribeRawZipkinTracer =
    tracerCache.getOrElseUpdate(scribeHost + scribePort + scribeCategory,
      apply(
        newClient(scribeHost, scribePort, clientName),
        scribeCategory,
        statsReceiver,
        timer
      )
    )

  /**
   * Creates a [[com.twitter.finagle.tracing.Tracer]] that sends traces to scribe with the specified
   * scribeCategory.
   *
   * @param scribeHost Host to send trace data to
   * @param scribePort Port to send trace data to
   * @param scribeCategory Category under which the trace data should be scribed
   * @param statsReceiver Where to log information about tracing success/failures
   * @param timer A Timer used for timing out spans in the [[DeadlineSpanMap]]
   */
  def apply(
    scribeHost: String,
    scribePort: Int,
    scribeCategory: String,
    statsReceiver: StatsReceiver,
    timer: Timer
  ): ScribeRawZipkinTracer =
    apply(scribeHost, scribePort, scribeCategory, statsReceiver, timer, s"$scribeCategory-tracer")

  /**
   * Creates a [[com.twitter.finagle.tracing.Tracer]] that sends traces to scribe with the specified
   * scribeCategory.
   *
   * @param client The scribe client used to send traces to scribe
   * @param scribeCategory Category under which the trace data should be scribed
   * @param statsReceiver Where to log information about tracing success/failures
   * @param timer A Timer used for timing out spans in the [[DeadlineSpanMap]]
   */
  def apply(
    client: Scribe.FutureIface,
    scribeCategory: String,
    statsReceiver: StatsReceiver,
    timer: Timer
  ): ScribeRawZipkinTracer =
    new ScribeRawZipkinTracer(client, statsReceiver.scope(scribeCategory), scribeCategory, timer)

  /**
   * Creates a [[com.twitter.finagle.tracing.Tracer]] that sends traces to Zipkin via scribe.
   *
   * @param scribeHost Host to send trace data to
   * @param scribePort Port to send trace data to
   * @param statsReceiver Where to log information about tracing success/failures
   * @param timer A Timer used for timing out spans in the [[DeadlineSpanMap]]
   */
  def apply(
    scribeHost: String = "localhost",
    scribePort: Int = 1463,
    statsReceiver: StatsReceiver = NullStatsReceiver,
    timer: Timer = DefaultTimer.twitter
  ): ScribeRawZipkinTracer =
    apply(scribeHost, scribePort, "zipkin", statsReceiver, timer, "zipkin-tracer")

  /**
   * Creates a [[com.twitter.finagle.tracing.Tracer]] that sends traces to Zipkin via scribe.
   *
   * @param client The scribe client used to send traces to scribe
   * @param statsReceiver Where to log information about tracing success/failures
   * @param timer A Timer used for timing out spans in the [[DeadlineSpanMap]]
   */
  def apply(
    client: Scribe.FutureIface,
    statsReceiver: StatsReceiver,
    timer: Timer
  ): ScribeRawZipkinTracer =
    apply(client, "zipkin", statsReceiver, timer)
}

/**
  * Receives traces and sends them off to scribe with the specified scribeCategory.
  *
  * @param client The scribe client used to send traces to scribe
  * @param statsReceiver We generate stats to keep track of traces sent, failures and so on
  * @param scribeCategory scribe category under which the trace will be logged
  * @param timer A Timer used for timing out spans in the [[DeadlineSpanMap]]
  * @param poolSize The number of Memory transports to make available for serializing Spans
  * @param initialBufferSize Initial size of each transport
  * @param maxBufferSize Max size to keep around. Transports will grow as needed, but will revert back to `initialBufferSize` when reset if
  * they grow beyond `maxBufferSize`
  */
private[thrift] class ScribeRawZipkinTracer(
  client: Scribe.FutureIface,
  statsReceiver: StatsReceiver,
  scribeCategory: String = "zipkin",
  timer: Timer = DefaultTimer.twitter,
  poolSize: Int = 10,
  initialBufferSize: StorageUnit = 512.bytes,
  maxBufferSize: StorageUnit = 1.megabyte
) extends RawZipkinTracer(statsReceiver, timer) {
  private[this] val scopedReceiver = statsReceiver.scope("log_span")
  private[this] val okCounter = scopedReceiver.counter("ok")
  private[this] val tryLaterCounter = scopedReceiver.counter("try_later")
  private[this] val errorReceiver = scopedReceiver.scope("error")

  private[this] val initialSizeInBytes = initialBufferSize.inBytes.toInt
  private[this] val maxSizeInBytes = maxBufferSize.inBytes.toInt

  /**
    * A wrapper around the TReusableMemoryTransport from Scrooge that
    * also resets the size of the underlying buffer if it grows larger
    * than `maxBufferSize`
    */
  private[this] val encoder = BaseEncoding.base64()
  private class ReusableTransport {
    private[this] val baos = new TByteArrayOutputStream(initialSizeInBytes) {
      private[this] val writer = new CharArrayWriter(initialSizeInBytes) {
        override def reset(): Unit = {
          super.reset()
          if (buf.length > maxSizeInBytes) {
            buf = new Array[Char](initialSizeInBytes)
          }
        }
      }
      @volatile private[this] var outStream = encoder.encodingStream(writer)

      override def reset(): Unit = {
        writer.reset()
        outStream = encoder.encodingStream(writer)
        super.reset()
      }

      override def write(bytes: Array[Byte], off: Int, len: Int): Unit = {
        outStream.write(bytes, off, len)
      }

      def toBase64Line(): String = {
        outStream.close()
        writer.write('\n')
        writer.toString()
      }
    }

    private[this] val transport = new TReusableMemoryTransport(baos)
    val protocol = Protocols.binaryFactory().getProtocol(transport)

    def reset(): Unit = transport.reset()
    def toBase64Line(): String = baos.toBase64Line()
  }

  private[this] val bufferPool = new ArrayBlockingQueue[ReusableTransport](poolSize)
  (0 until poolSize) foreach { _ => bufferPool.add(new ReusableTransport) }

  /**
    * Serialize the span, base64 encode and shove it all in a list.
    */
  private[this] def createLogEntries(spans: Seq[Span]): Seq[LogEntry] = {
    val entries = new ArrayBuffer[LogEntry](spans.size)

    spans foreach { span =>
      val transport = bufferPool.take()
      try {
        span.toThrift.write(transport.protocol)
        entries.append(LogEntry(category = scribeCategory, message = transport.toBase64Line()))
      } catch {
        case NonFatal(e) => errorReceiver.counter(e.getClass.getName).incr()
      } finally {
        transport.reset()
        bufferPool.add(transport)
      }
    }

    entries
  }

  /**
    * Log the span data via Scribe.
    */
  def sendSpans(spans: Seq[Span]): Future[Unit] = {
    client.log(createLogEntries(spans)).respond {
      case Return(ResultCode.Ok) => okCounter.incr()
      case Return(ResultCode.TryLater) => tryLaterCounter.incr()
      case Return(_) => Unit
      case Throw(e) => errorReceiver.counter(e.getClass.getName).incr()
    }.unit
  }
}

/**
  * Makes sure we don't trace the Scribe logging.
  */
private class TracelessFilter[Req, Rep] extends SimpleFilter[Req, Rep] {
  def apply(request: Req, service: Service[Req, Rep]): Future[Rep]  = {
    Trace.letClear {
      service(request)
    }
  }
}
