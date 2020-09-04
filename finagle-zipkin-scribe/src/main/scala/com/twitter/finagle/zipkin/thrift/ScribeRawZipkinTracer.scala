package com.twitter.finagle.zipkin.thrift

import com.twitter.conversions.StorageUnitOps._
import com.twitter.finagle.scribe.{Publisher, ScribeStats}
import com.twitter.finagle.stats.{DefaultStatsReceiver, StatsReceiver}
import com.twitter.finagle.thrift.Protocols
import com.twitter.finagle.thrift.scribe.thriftscala.{LogEntry, Scribe}
import com.twitter.finagle.util.DefaultTimer
import com.twitter.finagle.zipkin.core.{RawZipkinTracer, Span, TracerCache}
import com.twitter.finagle.zipkin.{host => Host}
import com.twitter.scrooge.TReusableMemoryTransport
import com.twitter.util._
import java.nio.charset.StandardCharsets
import java.util.concurrent.ArrayBlockingQueue
import java.util.{Arrays, Base64}
import org.apache.thrift.TByteArrayOutputStream
import org.apache.thrift.protocol.TProtocol
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

object ScribeRawZipkinTracer {
  private[this] val tracerCache = new TracerCache[ScribeRawZipkinTracer]
  private[this] val label = "zipkin-scribe"

  /**
   * Creates a [[com.twitter.finagle.tracing.Tracer]] that sends traces to Zipkin via scribe.
   *
   * @param scribeHost Host to send trace data to
   * @param scribePort Port to send trace data to
   * @param scribeCategory scribe category under which traces will be logged
   * @param statsReceiver Where to log information about tracing success/failures
   * @param label label to use for Scribe stats and the associated Finagle client
   * @param timer A Timer used for timing out spans in the [[DeadlineSpanMap]]
   */
  def apply(
    scribeHost: String = Host().getHostName,
    scribePort: Int = Host().getPort,
    scribeCategory: String = "zipkin",
    statsReceiver: StatsReceiver = DefaultStatsReceiver,
    label: String = label,
    timer: Timer = DefaultTimer
  ): ScribeRawZipkinTracer =
    tracerCache.getOrElseUpdate(
      scribeHost + scribePort + scribeCategory,
      apply(
        scribeCategory,
        Publisher.builder
          .withDest(s"inet!$scribeHost:$scribePort")
          .withStatsReceiver(statsReceiver)
          .build(scribeCategory, label),
        timer
      )
    )

  /**
   * Creates a [[com.twitter.finagle.tracing.Tracer]] that sends traces to scribe with the specified
   * scribeCategory.
   *
   * @param scribeCategory Category under which the trace data should be scribed
   * @param scribePublisher the [[com.twitter.finagle.scribe.Publisher]] to use for sending messages
   * @param timer A Timer used for timing out spans in the [[DeadlineSpanMap]]
   */
  def apply(
    scribeCategory: String,
    scribePublisher: Publisher,
    timer: Timer
  ): ScribeRawZipkinTracer =
    new ScribeRawZipkinTracer(scribeCategory, scribePublisher, timer)

  /**
   * Creates a [[com.twitter.finagle.tracing.Tracer]] that sends traces to scribe with the specified
   * scribeCategory.
   *
   * @param client the configured [[Scribe.MethodPerEndpoint]]
   * @param scribeCategory Category under which the trace data should be scribed
   * @param statsReceiver Where to log information about tracing success/failures
   * @param timer A Timer used for timing out spans in the [[DeadlineSpanMap]]
   */
  private[finagle] def apply(
    client: Scribe.MethodPerEndpoint,
    scribeCategory: String,
    statsReceiver: StatsReceiver,
    timer: Timer
  ): ScribeRawZipkinTracer =
    apply(
      scribeCategory,
      new Publisher(scribeCategory, new ScribeStats(statsReceiver), client),
      timer
    )
}

/**
 * Receives traces and sends them off to scribe with the specified scribeCategory.
 *
 * @param scribeCategory scribe category under which the trace will be logged
 * @param scribePublisher the [[com.twitter.finagle.scribe.Publisher]] to use for sending messages.
 * @param timer A Timer used for timing out spans in the [[DeadlineSpanMap]]
 * @param poolSize The number of Memory transports to make available for serializing Spans
 * @param initialBufferSize Initial size of each transport
 * @param maxBufferSize Max size to keep around. Transports will grow as needed, but will revert back to `initialBufferSize` when reset if
 * they grow beyond `maxBufferSize`
 */
private[finagle] class ScribeRawZipkinTracer(
  scribeCategory: String = "zipkin",
  scribePublisher: Publisher,
  timer: Timer = DefaultTimer,
  poolSize: Int = 10,
  initialBufferSize: StorageUnit = 512.bytes,
  maxBufferSize: StorageUnit = 1.megabyte)
    extends RawZipkinTracer(timer)
    with Closable {

  private[this] val initialSizeInBytes = initialBufferSize.inBytes.toInt
  private[this] val maxSizeInBytes = maxBufferSize.inBytes.toInt

  private class LimitedSizeByteArrayOutputStream(initSize: Int, maxSize: Int)
      extends TByteArrayOutputStream(initSize) {

    override def reset(): Unit = synchronized {
      if (buf.length > maxSize) {
        buf = new Array[Byte](maxSize)
      }
      super.reset()
    }
  }

  /**
   * Log the span data via Scribe.
   */
  def sendSpans(spans: Seq[Span]): Future[Unit] = {
    val logEntries = createLogEntries(spans)
    if (logEntries.isEmpty) Future.Done
    else scribePublisher.write(logEntries).unit
  }

  /**
   * Close the resource with the given deadline. This deadline is advisory,
   * giving the callee some leeway, for example to drain clients or finish
   * up other tasks.
   */
  override def close(deadline: Time): Future[Unit] =
    scribePublisher.close(deadline)

  /**
   * A wrapper around the TReusableMemoryTransport from Scrooge that
   * also resets the size of the underlying buffer if it grows larger
   * than `maxBufferSize`
   */
  private class ReusableTransport {
    private[this] val thriftOutput =
      new LimitedSizeByteArrayOutputStream(initialSizeInBytes, maxSizeInBytes)

    private[this] val transport = new TReusableMemoryTransport(thriftOutput)
    val protocol: TProtocol = Protocols.binaryFactory().getProtocol(transport)

    private[this] val base64Output =
      new LimitedSizeByteArrayOutputStream(initialSizeInBytes, maxSizeInBytes)

    def reset(): Unit = {
      transport.reset()
      base64Output.reset()
    }

    def toBase64Line: String = {
      // encode into a reusable OutputStream
      val out = Base64.getEncoder.wrap(base64Output)
      out.write(thriftOutput.get(), 0, thriftOutput.len())
      out.flush()
      out.close()

      // ensure there is space in the byte array for a trailing \n
      val encBytes = base64Output.get()
      val encLen = base64Output.len()
      val withNewline: Array[Byte] =
        if (encLen + 1 <= encBytes.length) encBytes
        else Arrays.copyOf(encBytes, encBytes.length + 1)

      // add the trailing '\n'
      withNewline(encLen) = '\n'
      new String(withNewline, 0, encLen + 1, StandardCharsets.US_ASCII)
    }
  }

  private[this] val bufferPool = new ArrayBlockingQueue[ReusableTransport](poolSize)
  0.until(poolSize).foreach { _ => bufferPool.add(new ReusableTransport) }

  /**
   * Serialize the span, base64 encode and shove it all in a list.
   */
  private[this] def createLogEntries(spans: Seq[Span]): Seq[LogEntry] = {
    val entries = new ArrayBuffer[LogEntry](spans.size)

    spans.foreach { span =>
      val transport = bufferPool.take()
      try {
        span.toThrift.write(transport.protocol)
        entries.append(LogEntry(category = scribeCategory, message = transport.toBase64Line))
      } catch {
        case NonFatal(e) => scribePublisher.handleError(e)
      } finally {
        transport.reset()
        bufferPool.add(transport)
      }
    }

    entries.toSeq // Scala 2.13 needs the `.toSeq` here.
  }
}
