package com.twitter.finagle.thrift

import com.twitter.finagle.builder.ClientBuilder
import java.net.InetSocketAddress
import org.apache.thrift.protocol.TBinaryProtocol
import com.twitter.finagle.tracing.{Span, TraceReceiver}
import java.util.ArrayList
import org.apache.scribe.{ResultCode, LogEntry, scribe}
import java.io.ByteArrayOutputStream
import sun.misc.BASE64Encoder
import com.twitter.finagle.{WriteException, Service}
import org.apache.thrift.transport.TIOStreamTransport
import com.twitter.finagle.stats.NullStatsReceiver

/**
 * Receives the Finagle generated traces, samples them
 * and sends off the survivors to BigBrotherBird via scribe.
 */
class BigBrotherBirdReceiver(client: scribe.ServiceToClient) extends TraceReceiver {

  private[this] var sampleRate = 10

  private[this] val encoder = new BASE64Encoder
  private[this] val protocolFactory = new TBinaryProtocol.Factory()
  private[this] val statsReceiver = NullStatsReceiver // TODO how to pick up whatever Finagle is using?

  private[this] val traceCategory = "b3"

  def this(scribeHost: String = "localhost", scribePort: Int = 1463) {
    this(new scribe.ServiceToClient(
      ClientBuilder()
      .hosts(new InetSocketAddress(scribeHost, scribePort))
      .codec(ThriftClientFramedCodec())
      .hostConnectionLimit(10)
      .build(),
    new TBinaryProtocol.Factory()))
  }

  /**
   * Set the sample rate.
   *
   * How much to let through? For everything, use 10000 = 100.00%
   * Default is 10 = 0.1% (let one in a 1000nd pass)
   */
  def setSampleRate(sr: Int) {
    if (sr < 0 || sr > 10000) {
      throw new IllegalArgumentException("Sample rate not within the valid range of 0-10000, was " + sr)
    }
    sampleRate = sr
  }

  /**
   * Should we drop this particular trace or send it on to Scribe?
   */
  private def shouldDropTrace(traceId: Long, sampleRate: Int): Boolean = {
    traceId % 10000 >= sampleRate
  }

  /**
   * Serialize the spans, base64 encode and shove it all in a list.
   */
  private def createLogEntries(span: Span): ArrayList[LogEntry] = {
    val msgs = new ArrayList[LogEntry]()

    val spans = new RichSpan(span).toThriftSpans
    spans.foreach{s =>
      val baos = new ByteArrayOutputStream
      s.write(protocolFactory.getProtocol(new TIOStreamTransport(baos)))
      val serializedBase64Span = encoder.encode(baos.toByteArray)
      msgs.add(new LogEntry().setCategory(traceCategory).setMessage(serializedBase64Span))
    }

    msgs
  }

  /**
   * Log the span data via Scribe.
   */
  def logSpan(span: Span) {
    client.Log(createLogEntries(span)) onSuccess {
      case ResultCode.OK => statsReceiver.counter("BigBrotherBirdReceiver-OK").incr()
      case ResultCode.TRY_LATER => statsReceiver.counter("BigBrotherBirdReceiver-TRY_LATER").incr()
      case _ => () /* ignore */
    } onFailure {
      case e: WriteException => statsReceiver.counter("BigBrotherBirdReceiver-WriteExecption").incr()
      // not much we can do about this exception dropping the message and moving on with our lives
    }
  }

  /**
   * Entry point for the spans.
   * Decide if we should drop it or send it to Scribe.
   */
  def receiveSpan(span: Span) {
    // TODO if the client is not connected to Scribe we should throw away the span

    if (shouldDropTrace(span.traceId, sampleRate)) {
      return
    }

    logSpan(span)
  }
}