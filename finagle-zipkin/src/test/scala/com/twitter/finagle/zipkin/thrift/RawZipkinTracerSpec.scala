package com.twitter.finagle.zipkin.thrift

import org.specs.{SpecificationWithJUnit, Specification}
import org.specs.mock.Mockito
import java.util.ArrayList

import com.twitter.conversions.time._
import com.twitter.util._
import com.twitter.finagle.tracing._
import com.twitter.finagle.util.{CloseNotifier, ManagedTimer, TwoTimer, Disposable}
import com.twitter.finagle.stats.NullStatsReceiver

import org.mockito.Matchers._
import java.net.{InetAddress, InetSocketAddress}
import com.twitter.finagle.service.TimeoutFilter

class RawZipkinTracerSpec extends SpecificationWithJUnit with Mockito {

  val traceId = TraceId(Some(SpanId(123)), Some(SpanId(123)), SpanId(123), None, Flags().setDebug)

  "RawZipkinTracer" should {
    "send all traces to scribe" in ManagedTimer.toTwitterTimer.foreach { timer =>
      val tracer = new RawZipkinTracer("localhost", 1463, NullStatsReceiver, Disposable.const(timer))
      tracer.client = mock[scribe.ServiceToClient]

      val expected = new ArrayList[LogEntry]()
      expected.add(new LogEntry().setCategory("zipkin")
        .setMessage("CgABAAAAAAAAAHsLAAMAAAAGbWV0aG9kCgAEAAAAAAAAAHsKAAUAAAAAAAAAew" +
        "8ABgwAAAAECgABAAAAAAdU1MALAAIAAAACY3IMAAMIAAEBAQEBBgACAAELAAMAAAAHc2Vydmlj" +
        "ZQAACgABAAAAAAdU1MALAAIAAAACY3MMAAMIAAEBAQEBBgACAAELAAMAAAAHc2VydmljZQAACg" +
        "ABAAAAAAdU1MALAAIAAAAGYm9vaG9vDAADCAABAQEBAQYAAgABCwADAAAAB3NlcnZpY2UACAAE" +
        "AA9CQAAKAAEAAAAAB1TUwAsAAgAAAANib28MAAMIAAEBAQEBBgACAAELAAMAAAAHc2VydmljZQ" +
        "AADwAIDAAAAAULAAEAAAADaTE2CwACAAAAAgAQCAADAAAAAgwABAgAAQEBAQEGAAIAAQsAAwAA" +
        "AAdzZXJ2aWNlAAALAAEAAAADaTMyCwACAAAABAAAACAIAAMAAAADDAAECAABAQEBAQYAAgABCw" +
        "ADAAAAB3NlcnZpY2UAAAsAAQAAAANpNjQLAAIAAAAIAAAAAAAAAEAIAAMAAAAEDAAECAABAQEB" +
        "AQYAAgABCwADAAAAB3NlcnZpY2UAAAsAAQAAAAZkb3VibGULAAIAAAAIQF7TMzMzMzMIAAMAAA" +
        "AFDAAECAABAQEBAQYAAgABCwADAAAAB3NlcnZpY2UAAAsAAQAAAAZzdHJpbmcLAAIAAAAGd29v" +
        "cGllCAADAAAABgwABAgAAQEBAQEGAAIAAQsAAwAAAAdzZXJ2aWNlAAACAAkBAA=="))
      tracer.client.Log(anyObject()) returns Future(ResultCode.OK)

      val inetAddress = InetAddress.getByAddress(Array.fill(4) {
        1
      })
      tracer.record(Record(traceId, Time.fromSeconds(123),
        Annotation.ClientAddr(new InetSocketAddress(inetAddress, 1))))
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

      there was one(tracer.client).Log(expected)
    }

    "register onClose handler that calls release" in {
      var callback: () => Unit = null
      val factory = RawZipkinTracer()
      val tracer = factory(new CloseNotifier {
        def onClose(h: => Unit) = callback = () => h
      }).asInstanceOf[RawZipkinTracer]

      tracer.client mustNot beNull
      callback()
      // tracer.release() should set client to null
      tracer.client must beNull
    }

    "logSpan if a timeout occurs" in ManagedTimer.toTwitterTimer.foreach { timer =>
      val ann1 = Annotation.Message("some_message")
      val ann2 = Annotation.Rpcname("some_service", "rpc_name")
      val ann3 = Annotation.Message(TimeoutFilter.TimeoutAnnotation)

      val tracer = new RawZipkinTracer("localhost", 1463, NullStatsReceiver, Disposable.const(timer))
      tracer.client = mock[scribe.ServiceToClient]

      tracer.client.Log(anyObject()) returns Future(ResultCode.OK)

      tracer.record(Record(traceId, Time.fromSeconds(1), ann1))
      tracer.record(Record(traceId, Time.fromSeconds(2), ann2))
      tracer.record(Record(traceId, Time.fromSeconds(3), ann3))

      // scribe Log method is in java
      there was one(tracer.client).Log(any[java.util.List[LogEntry]])
    }
  }
}
