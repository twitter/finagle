package com.twitter.finagle.zipkin.thrift

import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito

import com.twitter.conversions.time._
import com.twitter.util._
import com.twitter.finagle.tracing._
import com.twitter.finagle.stats.NullStatsReceiver

import org.mockito.Matchers._
import java.net.{InetAddress, InetSocketAddress}
import com.twitter.finagle.service.TimeoutFilter

class RawZipkinTracerSpec extends SpecificationWithJUnit with Mockito {

  val traceId = TraceId(Some(SpanId(123)), Some(SpanId(123)), SpanId(123), None, Flags().setDebug)

  "RawZipkinTracer" should {
    "send all traces to scribe" in {
      val tracer = new RawZipkinTracer("localhost", 1463, NullStatsReceiver) {
        override val client = mock[scribe.FinagledClient]
      }

      val expected = new LogEntry(
        category = "zipkin",
        message = "CgABAAAAAAAAAHsLAAMAAAAGbWV0aG9kCgAEAAAAAAAAAHsKAAUAAAAAA" +
          "AAAew8ABgwAAAAECgABAAAAAAdU1MALAAIAAAACY3IMAAMIAAEBAQEBBgACAVkLAA" +
          "MAAAAHc2VydmljZQAACgABAAAAAAdU1MALAAIAAAACY3MMAAMIAAEBAQEBBgACAVk" +
          "LAAMAAAAHc2VydmljZQAACgABAAAAAAdU1MALAAIAAAAGYm9vaG9vDAADCAABAQEB" +
          "AQYAAgFZCwADAAAAB3NlcnZpY2UACAAEAA9CQAAKAAEAAAAAB1TUwAsAAgAAAANib" +
          "28MAAMIAAEBAQEBBgACAVkLAAMAAAAHc2VydmljZQAADwAIDAAAAAcLAAEAAAACY2" +
          "ELAAIAAAABAQgAAwAAAAAMAAQIAAEBAQEBBgACAVkLAAMAAAAHc2VydmljZQAACwA" +
          "BAAAAAnNhCwACAAAAAQEIAAMAAAAADAAECAABCgoKCgYAAh+QCwADAAAAB3NlcnZp" +
          "Y2UAAAsAAQAAAANpMTYLAAIAAAACABAIAAMAAAACDAAECAABAQEBAQYAAgFZCwADA" +
          "AAAB3NlcnZpY2UAAAsAAQAAAANpMzILAAIAAAAEAAAAIAgAAwAAAAMMAAQIAAEBAQ" +
          "EBBgACAVkLAAMAAAAHc2VydmljZQAACwABAAAAA2k2NAsAAgAAAAgAAAAAAAAAQAg" +
          "AAwAAAAQMAAQIAAEBAQEBBgACAVkLAAMAAAAHc2VydmljZQAACwABAAAABmRvdWJs" +
          "ZQsAAgAAAAhAXtMzMzMzMwgAAwAAAAUMAAQIAAEBAQEBBgACAVkLAAMAAAAHc2Vyd" +
          "mljZQAACwABAAAABnN0cmluZwsAAgAAAAZ3b29waWUIAAMAAAAGDAAECAABAQEBAQ" +
          "YAAgFZCwADAAAAB3NlcnZpY2UAAAIACQEA\n")
      tracer.client.log(anyObject()) returns Future(ResultCode.Ok)

      val localAddress = InetAddress.getByAddress(Array.fill(4) { 1 })
      val remoteAddress = InetAddress.getByAddress(Array.fill(4) { 10 })
      tracer.record(Record(traceId, Time.fromSeconds(123), Annotation.ClientAddr(new InetSocketAddress(localAddress, 345))))
      tracer.record(Record(traceId, Time.fromSeconds(123), Annotation.LocalAddr(new InetSocketAddress(localAddress, 345))))
      tracer.record(Record(traceId, Time.fromSeconds(123), Annotation.ServerAddr(new InetSocketAddress(remoteAddress, 8080))))
      tracer.record(Record(traceId, Time.fromSeconds(123), Annotation.Rpcname("service", "method")))
      tracer.record(Record(traceId, Time.fromSeconds(123), Annotation.BinaryAnnotation("i16", 16.toShort)))
      tracer.record(Record(traceId, Time.fromSeconds(123), Annotation.BinaryAnnotation("i32", 32)))
      tracer.record(Record(traceId, Time.fromSeconds(123), Annotation.BinaryAnnotation("i64", 64L)))
      tracer.record(Record(traceId, Time.fromSeconds(123), Annotation.BinaryAnnotation("double", 123.3d)))
      tracer.record(Record(traceId, Time.fromSeconds(123), Annotation.BinaryAnnotation("string", "woopie")))
      tracer.record(Record(traceId, Time.fromSeconds(123), Annotation.Message("boo")))
      tracer.record(Record(traceId, Time.fromSeconds(123), Annotation.Message("boohoo"), Some(1.second)))
      tracer.record(Record(traceId, Time.fromSeconds(123), Annotation.ClientSend()))
      tracer.record(Record(traceId, Time.fromSeconds(123), Annotation.ClientRecv()))

      there was one(tracer.client).log(Seq(expected))
    }

    "logSpan if a timeout occurs" in {
      val ann1 = Annotation.Message("some_message")
      val ann2 = Annotation.Rpcname("some_service", "rpc_name")
      val ann3 = Annotation.Message(TimeoutFilter.TimeoutAnnotation)

      val tracer = new RawZipkinTracer("localhost", 1463, NullStatsReceiver) {
        override val client = mock[scribe.FinagledClient]
      }

      tracer.client.log(anyObject()) returns Future(ResultCode.Ok)

      tracer.record(Record(traceId, Time.fromSeconds(1), ann1))
      tracer.record(Record(traceId, Time.fromSeconds(2), ann2))
      tracer.record(Record(traceId, Time.fromSeconds(3), ann3))

      // scribe Log method is in java
      there was one(tracer.client).log(any[Seq[LogEntry]])
    }
  }
}
