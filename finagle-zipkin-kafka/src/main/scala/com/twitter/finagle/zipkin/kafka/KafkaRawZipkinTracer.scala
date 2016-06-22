package com.twitter.finagle.zipkin.kafka

import com.twitter.conversions.storage._
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.thrift.Protocols
import com.twitter.finagle.util.DefaultTimer
import com.twitter.finagle.zipkin.core.{Span, RawZipkinTracer}
import com.twitter.util._
import java.util
import org.apache.kafka.clients.producer._
import org.apache.thrift.protocol.{TBinaryProtocol, TList, TType}
import org.apache.thrift.transport.TMemoryBuffer

private[kafka] object KafkaRawZipkinTracer {
  type ZipkinProducer = Producer[Array[Byte], Array[Byte]]
  type ZipkinProducerRecord = ProducerRecord[Array[Byte], Array[Byte]]
}
import KafkaRawZipkinTracer._

/**
  * Receives the Finagle generated traces and sends them off to Zipkin via Kafka.
  *
  * @param producer kafka producer
  * @param topic kafka topic to send to
  * @param statsReceiver see [[RawZipkinTracer.statsReceiver]]
  * @param timer see [[RawZipkinTracer.timer]]
  */
private[kafka] class KafkaRawZipkinTracer(
  producer: ZipkinProducer,
  topic: String,
  statsReceiver: StatsReceiver,
  timer: Timer = DefaultTimer.twitter,
  initialBufferSize: StorageUnit = 512.bytes
) extends RawZipkinTracer(statsReceiver, timer) {
  // XXX(sveinnfannar): Should we override RawZipkinTracer#flush to make sure
  //                    the kafka producer sends all queued messages

  private[this] val okCounter = statsReceiver.scope("log_span").counter("ok")
  private[this] val errorReceiver = statsReceiver.scope("log_span").scope("error")

  private[kafka] lazy val onSendSuccess = new Callback {
    override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit =
      if (exception == null) okCounter.incr()
      else errorReceiver.counter(exception.getClass.getName).incr()
  }
  private[this] val onSendFailure: Throwable => Unit =
      e => errorReceiver.counter(e.getClass.getName).incr()

  private[this] val protocolFactory = Protocols.binaryFactory(statsReceiver = statsReceiver)

  private[this] val initialSizeInBytes = initialBufferSize.inBytes.toInt

  def sendSpans(spans: Seq[Span]): Future[Unit] = {
    Try {
      val serializedSpans = spansToThriftByteArray(spans)

      // Producer#send appends the message to an internal queue and returns immediately.
      // Messages are then batch sent asynchronously on another thread and the callback invoked.
      producer.send(new ProducerRecord(topic, serializedSpans), onSendSuccess)
    }.onFailure(onSendFailure)

    Future.Unit
  }

  private[this] def spansToThriftByteArray(spans: Seq[Span]): Array[Byte] = {
    // serialize all spans as a thrift list
    val transport = new TMemoryBuffer(initialSizeInBytes) // bytes
    val protocol = protocolFactory.getProtocol(transport)
    protocol.writeListBegin(new TList(TType.STRUCT, spans.size))
    spans.foreach(_.toThrift.write(protocol))
    protocol.writeListEnd()
    transport.getArray
  }
}
