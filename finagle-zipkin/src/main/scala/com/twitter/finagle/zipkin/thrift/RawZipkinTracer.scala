package com.twitter.finagle.zipkin.thrift

import com.google.common.io.BaseEncoding
import com.twitter.conversions.storage._
import com.twitter.conversions.time._
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.stats.{ClientStatsReceiver, NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.thrift.{Protocols, ThriftClientFramedCodec, thrift}
import com.twitter.finagle.tracing._
import com.twitter.finagle.util.DefaultTimer
import com.twitter.finagle.zipkin.thriftscala._
import com.twitter.finagle.{Service, SimpleFilter, tracing}
import com.twitter.scrooge.TReusableMemoryTransport
import com.twitter.util._
import java.io.CharArrayWriter
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.util.concurrent.{ArrayBlockingQueue, TimeoutException}
import org.apache.thrift.TByteArrayOutputStream
import scala.collection.mutable.{ArrayBuffer, HashMap, SynchronizedMap}
import scala.language.reflectiveCalls

object RawZipkinTracer {
  private[this] def newClient(
    scribeHost: String,
    scribePort: Int
  ): Scribe.FutureIface = {
    val transport = ClientBuilder()
      .name("zipkin-tracer")
      .hosts(new InetSocketAddress(scribeHost, scribePort))
      .codec(ThriftClientFramedCodec())
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

  // to make sure we only create one instance of the tracer per host and port
  private[this] val map =
    new HashMap[String, RawZipkinTracer] with SynchronizedMap[String, RawZipkinTracer]

  /**
   * @param scribeHost Host to send trace data to
   * @param scribePort Port to send trace data to
   * @param statsReceiver Where to log information about tracing success/failures
   */
  def apply(
    scribeHost: String = "localhost",
    scribePort: Int = 1463,
    statsReceiver: StatsReceiver = NullStatsReceiver,
    timer: Timer = DefaultTimer.twitter
  ): RawZipkinTracer = synchronized {
    map.getOrElseUpdate(scribeHost + ":" + scribePort, apply(newClient(scribeHost, scribePort), statsReceiver, timer))
  }

  def apply(client: Scribe.FutureIface, statsReceiver: StatsReceiver, timer: Timer): RawZipkinTracer =
    new RawZipkinTracer(client, statsReceiver.scope("zipkin"), timer)

  // Try to flush the tracers when we shut
  // down. We give it 100ms.
  Runtime.getRuntime().addShutdownHook(new Thread {
    setName("RawZipkinTracer-ShutdownHook")
    override def run() {
      val tracers = RawZipkinTracer.synchronized(map.values.toSeq)
      val joined = Future.join(tracers map(_.flush()))
      try {
        Await.result(joined, 100.milliseconds)
      } catch {
        case _: TimeoutException =>
          System.err.println("Failed to flush all traces before quitting")
      }
    }
  })
}

/**
 * Receives the Finagle generated traces and sends them off to Zipkin via scribe.
 * @param client The scribe client used to send traces to scribe
 * @param statsReceiver We generate stats to keep track of traces sent, failures and so on
 * @param timer A Timer used for timing out spans in the [[DeadlineSpanMap]]
 * @param poolSize The number of Memory transports to make available for serializing Spans
 * @param initialBufferSize Initial size of each transport
 * @param maxBufferSize Max size to keep around. Transports will grow as needed, but will revert back to `initialBufferSize` when reset if
 * they grow beyond `maxBufferSize`
 */
private[thrift] class RawZipkinTracer(
  client: Scribe.FutureIface,
  statsReceiver: StatsReceiver,
  timer: Timer = DefaultTimer.twitter,
  poolSize: Int = 10,
  initialBufferSize: StorageUnit = 512.bytes,
  maxBufferSize: StorageUnit = 1.megabyte
) extends Tracer {
  private[this] val TraceCategory = "zipkin" // scribe category

  private[this] val ErrorAnnotation = "%s: %s" // annotation: errorMessage

  // this sends off spans after the deadline is hit, no matter if it ended naturally or not.
  private[this] val spanMap: DeadlineSpanMap =
    new DeadlineSpanMap(logSpans(_), 120.seconds, statsReceiver, timer)

  private[this] val scopedReceiver = statsReceiver.scope("log_span")
  private[this] val okCounter = scopedReceiver.counter("ok")
  private[this] val tryLaterCounter = scopedReceiver.counter("try_later")
  private[this] val errorReceiver = scopedReceiver.scope("error")

  protected[thrift] def flush() = spanMap.flush()

  /**
   * Always sample the request.
   */
  def sampleTrace(traceId: TraceId): Option[Boolean] = Some(true)

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

    def reset() = transport.reset()
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
        entries.append(LogEntry(category = TraceCategory, message = transport.toBase64Line()))
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
  def logSpans(spans: Seq[Span]): Future[Unit] = {
    client.log(createLogEntries(spans)).respond {
      case Return(ResultCode.Ok) => okCounter.incr()
      case Return(ResultCode.TryLater) => tryLaterCounter.incr()
      case Throw(e) => errorReceiver.counter(e.getClass.getName).incr()
    }.unit
  }

  /**
   * Mutate the Span with whatever new info we have.
   * If we see an "end" annotation we remove the span and send it off.
   */
  protected def mutate(traceId: TraceId)(f: MutableSpan => Unit) {
    spanMap.update(traceId)(f)
  }

  private[this] val TrueBB: ByteBuffer = ByteBuffer.wrap(Array[Byte](1))
  private[this] val FalseBB: ByteBuffer = ByteBuffer.wrap(Array[Byte](0))

  def record(record: Record) {
    record.annotation match {
      case tracing.Annotation.WireSend =>
        annotate(record, thrift.Constants.WIRE_SEND)
      case tracing.Annotation.WireRecv =>
        annotate(record, thrift.Constants.WIRE_RECV)
      case tracing.Annotation.WireRecvError(error: String) =>
        annotate(record, ErrorAnnotation.format(thrift.Constants.WIRE_RECV_ERROR, error))
      case tracing.Annotation.ClientSend() =>
        annotate(record, thrift.Constants.CLIENT_SEND)
      case tracing.Annotation.ClientRecv() =>
        annotate(record, thrift.Constants.CLIENT_RECV)
      case tracing.Annotation.ClientRecvError(error: String) =>
        annotate(record, ErrorAnnotation.format(thrift.Constants.CLIENT_RECV_ERROR, error))
      case tracing.Annotation.ServerSend() =>
        annotate(record, thrift.Constants.SERVER_SEND)
      case tracing.Annotation.ServerRecv() =>
        annotate(record, thrift.Constants.SERVER_RECV)
      case tracing.Annotation.ServerSendError(error: String) =>
        annotate(record, ErrorAnnotation.format(thrift.Constants.SERVER_SEND_ERROR, error))
      case tracing.Annotation.ClientSendFragment() =>
        annotate(record, thrift.Constants.CLIENT_SEND_FRAGMENT)
      case tracing.Annotation.ClientRecvFragment() =>
        annotate(record, thrift.Constants.CLIENT_RECV_FRAGMENT)
      case tracing.Annotation.ServerSendFragment() =>
        annotate(record, thrift.Constants.SERVER_SEND_FRAGMENT)
      case tracing.Annotation.ServerRecvFragment() =>
        annotate(record, thrift.Constants.SERVER_RECV_FRAGMENT)
      case tracing.Annotation.Message(value) =>
        annotate(record, value)
      case tracing.Annotation.Rpc(name: String) =>
        spanMap.update(record.traceId)(_.setName(name))
      case tracing.Annotation.ServiceName(serviceName: String) =>
        spanMap.update(record.traceId)(_.setServiceName(serviceName))
      case tracing.Annotation.Rpcname(service: String, rpc: String) =>
        spanMap.update(record.traceId)(_.setServiceName(service).setName(rpc))
      case tracing.Annotation.BinaryAnnotation(key: String, value: Boolean) =>
        binaryAnnotation(record, key, (if (value) TrueBB else FalseBB).duplicate(), thrift.AnnotationType.BOOL)
      case tracing.Annotation.BinaryAnnotation(key: String, value: Array[Byte]) =>
        binaryAnnotation(record, key, ByteBuffer.wrap(value), thrift.AnnotationType.BYTES)
      case tracing.Annotation.BinaryAnnotation(key: String, value: ByteBuffer) =>
        binaryAnnotation(record, key, value, thrift.AnnotationType.BYTES)
      case tracing.Annotation.BinaryAnnotation(key: String, value: Short) =>
        binaryAnnotation(record, key, ByteBuffer.allocate(2).putShort(0, value), thrift.AnnotationType.I16)
      case tracing.Annotation.BinaryAnnotation(key: String, value: Int) =>
        binaryAnnotation(record, key, ByteBuffer.allocate(4).putInt(0, value), thrift.AnnotationType.I32)
      case tracing.Annotation.BinaryAnnotation(key: String, value: Long) =>
        binaryAnnotation(record, key, ByteBuffer.allocate(8).putLong(0, value), thrift.AnnotationType.I64)
      case tracing.Annotation.BinaryAnnotation(key: String, value: Double) =>
        binaryAnnotation(record, key, ByteBuffer.allocate(8).putDouble(0, value), thrift.AnnotationType.DOUBLE)
      case tracing.Annotation.BinaryAnnotation(key: String, value: String) =>
        binaryAnnotation(record, key, ByteBuffer.wrap(value.getBytes), thrift.AnnotationType.STRING)
      case tracing.Annotation.BinaryAnnotation(key: String, value) => // Throw error?
      case tracing.Annotation.LocalAddr(ia: InetSocketAddress) =>
        setEndpoint(record, ia)
      case tracing.Annotation.ClientAddr(ia: InetSocketAddress) =>
        // use a binary annotation over a regular annotation to avoid a misleading timestamp
        spanMap.update(record.traceId) { _.addBinaryAnnotation(BinaryAnnotation(
          thrift.Constants.CLIENT_ADDR,
          TrueBB.duplicate(),
          thrift.AnnotationType.BOOL,
          Endpoint.fromSocketAddress(ia)))
        }
      case tracing.Annotation.ServerAddr(ia: InetSocketAddress) =>
        spanMap.update(record.traceId) { _.addBinaryAnnotation(BinaryAnnotation(
          thrift.Constants.SERVER_ADDR,
          TrueBB.duplicate(),
          thrift.AnnotationType.BOOL,
          Endpoint.fromSocketAddress(ia)))
        }
    }
  }

  /**
   * Sets the endpoint in the span for any future annotations. Also
   * sets the endpoint in any previous annotations that lack one.
   */
  protected def setEndpoint(record: Record, ia: InetSocketAddress) {
    spanMap.update(record.traceId)(_.setEndpoint(Endpoint.fromSocketAddress(ia).boundEndpoint))
  }

  protected def binaryAnnotation(
    record: Record,
    key: String,
    value: ByteBuffer,
    annotationType: thrift.AnnotationType
  ) {
    spanMap.update(record.traceId) { span =>
      span.addBinaryAnnotation(BinaryAnnotation(key, value, annotationType, span.endpoint))
    }
  }

  /**
   * Add this record as a time based annotation.
   */
  protected def annotate(record: Record, value: String) {
    spanMap.update(record.traceId) { span =>
      span.addAnnotation(ZipkinAnnotation(record.timestamp, value, span.endpoint))
    }
  }
}

/**
 * Makes sure we don't trace the Scribe logging.
 */
private class TracelessFilter[Req, Rep] extends SimpleFilter[Req, Rep] {
  def apply(request: Req, service: Service[Req, Rep]) = {
    Trace.letClear {
      service(request)
    }
  }
}
