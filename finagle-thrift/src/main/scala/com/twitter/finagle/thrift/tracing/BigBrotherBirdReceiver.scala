package com.twitter.finagle.thrift.tracing

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
import com.twitter.finagle.thrift.{RichSpan, ThriftClientFramedCodec, ThriftClientRequest}
import com.twitter.finagle.stats.NullStatsReceiver

/**
 * Receives the Finagle generated traces, samples them
 * and sends off the survivors to BigBrotherBird via scribe.
 */
class BigBrotherBirdReceiver(scribeHost: String = "localhost", scribePort: Int = 1463) extends TraceReceiver {

  // how much to let through? for everything, use 10000 = 100.00%
  // default is 10 = 0.1% (let one in a 1000nd pass)
  var sampleRate = 10

  private val encoder = new BASE64Encoder
  private val protocolFactory = new TBinaryProtocol.Factory()
  private val statsReceiver = NullStatsReceiver // TODO how to pick up whatever Finagle is using?

  private val traceCategory = "b3"

  private val service: Service[ThriftClientRequest, Array[Byte]] = ClientBuilder()
    .hosts(new InetSocketAddress(scribeHost, scribePort))
    .codec(ThriftClientFramedCodec())
    .build()

  private val client = new scribe.ServiceToClient(service, new TBinaryProtocol.Factory())


  /**
   * Should we drop this particular trace or send it on to Scribe?
   */
  private def shouldDropTrace(traceId: Long, sampleRate: Int) : Boolean = {
    traceId % 10000 >= sampleRate
  }

  /**
   * Serialize the spans, base64 encode and shove it all in a list.
   */
  private def createLogEntries(span: Span) : ArrayList[LogEntry] = {
    val msgs = new ArrayList[LogEntry]()

    val spans = new RichSpan(span).toThriftSpans
    spans.foreach(s => {
      val baos = new ByteArrayOutputStream
      s.write(protocolFactory.getProtocol(new TIOStreamTransport(baos)))
      val serializedBase64Span = encoder.encode(baos.toByteArray)
      msgs.add(new LogEntry().setCategory(traceCategory).setMessage(serializedBase64Span))
    })

    msgs
  }

  /**
   * Log the span data via Scribe.
   */
  def logSpan(span: Span) {
    try {
      client.Log(createLogEntries(span)) onSuccess {
        response =>
          if (ResultCode.OK == response) {
            statsReceiver.counter("BigBrotherBirdReceiver-OK").incr()
          } else if (ResultCode.TRY_LATER == response) {
            statsReceiver.counter("BigBrotherBirdReceiver-TRY_LATER").incr()
          }
      }
    } catch {
      case e: WriteException => {
        statsReceiver.counter("BigBrotherBirdReceiver-WriteExecption").incr()
        // TODO this exception doesn't seem to be thrown even though there is no scribe running. what's up?
        // not much we can do about this exception
        // dropping the message and moving on with our lives
      }
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