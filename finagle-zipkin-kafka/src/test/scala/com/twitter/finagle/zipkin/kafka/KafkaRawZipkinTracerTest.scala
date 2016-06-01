package com.twitter.finagle.zipkin.kafka

import java.util
import java.util.concurrent.{Future => JFuture, TimeUnit}

import com.twitter.finagle.stats.{NullStatsReceiver, InMemoryStatsReceiver}
import com.twitter.finagle.tracing.{Flags, SpanId, TraceId}
import com.twitter.finagle.zipkin.core.{BinaryAnnotation, Span, ZipkinAnnotation, Endpoint}
import com.twitter.finagle.zipkin.kafka.KafkaRawZipkinTracer.{ZipkinProducerRecord, ZipkinProducer}
import com.twitter.util.{Base64StringEncoder, Time}
import org.apache.kafka.clients.producer.{RecordMetadata, Callback, ProducerRecord, Producer}
import org.apache.kafka.common.{PartitionInfo, Metric, MetricName}

import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

@RunWith(classOf[JUnitRunner])
class KafkaRawZipkinTracerTest extends FunSuite {

  val traceId = TraceId(Some(SpanId(123)), Some(SpanId(123)), SpanId(123), None, Flags().setDebug)

  test("send properly formulated messages to kafka") {
    val topic = "zipkin-test"
    val producer = new TestProducer
    val statsReceiver = new InMemoryStatsReceiver
    val tracer = new KafkaRawZipkinTracer(producer, topic, statsReceiver)

    val localEndpoint = Endpoint(2323, 23)
    val remoteEndpoint = Endpoint(333, 22)

    val annotations = Seq(
      ZipkinAnnotation(Time.fromSeconds(123), "cs", localEndpoint),
      ZipkinAnnotation(Time.fromSeconds(126), "cr", localEndpoint),
      ZipkinAnnotation(Time.fromSeconds(123), "ss", remoteEndpoint),
      ZipkinAnnotation(Time.fromSeconds(124), "sr", remoteEndpoint),
      ZipkinAnnotation(Time.fromSeconds(123), "llamas", localEndpoint)
    )

    val span = Span(
      traceId = traceId,
      annotations = annotations,
      _serviceName = Some("hickupquail"),
      _name=Some("foo"),
      bAnnotations = Seq.empty[BinaryAnnotation],
      endpoint = localEndpoint)

    val expected = Base64StringEncoder.decode("DAAAAAEKAAEAAAAAAAAAews" +
      "AAwAAAANmb28KAAQAAAAAAAAAewoABQAAAAAAAAB7DwAGDAAAAAUKAAEAAAAAB1" +
      "TUwAsAAgAAAAJjcwwAAwgAAQAACRMGAAIAFwsAAwAAAAtoaWNrdXBxdWFpbAAAC" +
      "gABAAAAAAeCm4ALAAIAAAACY3IMAAMIAAEAAAkTBgACABcLAAMAAAALaGlja3Vw" +
      "cXVhaWwAAAoAAQAAAAAHVNTACwACAAAAAnNzDAADCAABAAABTQYAAgAWCwADAAA" +
      "AC2hpY2t1cHF1YWlsAAAKAAEAAAAAB2QXAAsAAgAAAAJzcgwAAwgAAQAAAU0GAA" +
      "IAFgsAAwAAAAtoaWNrdXBxdWFpbAAACgABAAAAAAdU1MALAAIAAAAGbGxhbWFzD" +
      "AADCAABAAAJEwYAAgAXCwADAAAAC2hpY2t1cHF1YWlsAAACAAkBAAAAAAAAAAAA" +
      "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" +
      "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" +
      "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" +
      "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" +
      "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" +
      "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" +
      "AAAAAAAAAAAA==")

    tracer.sendSpans(Seq(span))

    assert(producer.messages.length == 1)
    val msg = producer.messages.head
    assert(msg.topic == topic)
    assert(util.Arrays.equals(msg.value, expected))
    assert(statsReceiver.counters == Map(List("log_span", "ok") -> 1))
  }

  test("increment error counter on synchronous failure") {
    val topic = "zipkin-test"
    val statsReceiver = new InMemoryStatsReceiver
    val producer = new TestProducer {
      override def send(record: ZipkinProducerRecord, callback: Callback): JFuture[RecordMetadata] = {
        callback.onCompletion(null, new TestProducerException())
        null
      }
    }
    val tracer = new KafkaRawZipkinTracer(producer, topic, statsReceiver)

    val span = Span(
      traceId = traceId,
      annotations = Seq.empty,
      _serviceName = Some("hickupquail"),
      _name=Some("foo"),
      bAnnotations = Seq.empty[BinaryAnnotation],
      endpoint = Endpoint(2323, 23))

    tracer.sendSpans(Seq(span))

    assert(statsReceiver.counters ==
      Map(List("log_span", "error",
        "com.twitter.finagle.zipkin.kafka.TestProducerException") -> 1))
  }

  test("increment error counter on asynchronous failure") {
    val topic = "zipkin-test"
    val statsReceiver = new InMemoryStatsReceiver
    val producer = new TestProducer {
      override def send(record: ZipkinProducerRecord, callback: Callback): JFuture[RecordMetadata] = {
        throw new TestProducerException()
      }
    }
    val tracer = new KafkaRawZipkinTracer(producer, topic, statsReceiver)

    val span = Span(
      traceId = traceId,
      annotations = Seq.empty,
      _serviceName = Some("hickupquail"),
      _name=Some("foo"),
      bAnnotations = Seq.empty[BinaryAnnotation],
      endpoint = Endpoint(2323, 23))

    tracer.sendSpans(Seq(span))

    assert(statsReceiver.counters ==
      Map(List("log_span", "error",
        "com.twitter.finagle.zipkin.kafka.TestProducerException") -> 1))
  }
}

final class TestProducerException extends Exception

private[this] class TestProducer extends ZipkinProducer {
  var messages: Seq[ZipkinProducerRecord] = Seq.empty
  override def partitionsFor(topic: String): util.List[PartitionInfo] = ???
  override def metrics(): util.Map[MetricName, _ <: Metric] = ???
  override def send(record: ZipkinProducerRecord): JFuture[RecordMetadata] = ???
  override def send(record: ZipkinProducerRecord, callback: Callback): JFuture[RecordMetadata] = {
    messages ++= Seq(record)
    callback.onCompletion(null, null)
    null
  }
  override def close(): Unit = ???
}

