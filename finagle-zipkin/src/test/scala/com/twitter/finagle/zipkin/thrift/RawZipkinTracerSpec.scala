package com.twitter.finagle.zipkin.thrift

import org.specs.Specification
import org.specs.mock.Mockito
import java.util.ArrayList

import com.twitter.util._
import com.twitter.finagle.tracing._
import com.twitter.finagle.util.{CloseNotifier, FinagleTimer}
import com.twitter.finagle.stats.NullStatsReceiver

import org.mockito.Matchers._
import java.nio.ByteBuffer
import java.net.{InetAddress, InetSocketAddress}
import com.twitter.finagle.service.TimeoutFilter

object RawZipkinTracerSpec extends Specification with Mockito {

  val traceId = TraceId(Some(SpanId(123)), Some(SpanId(123)), SpanId(123), None)

  "RawZipkinTracer" should {
    val zipkinTimer = FinagleTimer.getManaged.make()
    doAfter { zipkinTimer.dispose() }

    "send all traces to scribe" in {
      val tracer = new RawZipkinTracer("localhost", 1463, NullStatsReceiver, zipkinTimer)
      tracer.client = mock[scribe.ServiceToClient]

      val expected = new ArrayList[LogEntry]()
      expected.add(new LogEntry().setCategory("zipkin")
        .setMessage("CgABAAAAAAAAAHsLAAMAAAAGbWV0aG9kCgAEAAAAAAAAAHsKAAUAAAAAAAAAew" +
        "8ABgwAAAACCgABAAAAAAdU1MALAAIAAAACY3MMAAMIAAEBAQEBBgACAAELAAMAAAAHc2Vydmlj" +
        "ZQAACgABAAAAAAdU1MALAAIAAAACY3IMAAMIAAEBAQEBBgACAAELAAMAAAAHc2VydmljZQAADw" +
        "AIDAAAAAULAAEAAAADaTE2CwACAAAAAgAQCAADAAAAAgwABAgAAQEBAQEGAAIAAQsAAwAAAAdz" +
        "ZXJ2aWNlAAALAAEAAAADaTMyCwACAAAABAAAACAIAAMAAAADDAAECAABAQEBAQYAAgABCwADAA" +
        "AAB3NlcnZpY2UAAAsAAQAAAANpNjQLAAIAAAAIAAAAAAAAAEAIAAMAAAAEDAAECAABAQEBAQYA" +
        "AgABCwADAAAAB3NlcnZpY2UAAAsAAQAAAAZkb3VibGULAAIAAAAIQF7TMzMzMzMIAAMAAAAFDA" +
        "AECAABAQEBAQYAAgABCwADAAAAB3NlcnZpY2UAAAsAAQAAAAZzdHJpbmcLAAIAAAAGd29vcGll" +
        "CAADAAAABgwABAgAAQEBAQEGAAIAAQsAAwAAAAdzZXJ2aWNlAAAA"))
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

    "logSpan if a timeout occurs" in {
      val ann1 = Annotation.Message("some_message")
      val ann2 = Annotation.Rpcname("some_service", "rpc_name")
      val ann3 = Annotation.Message(TimeoutFilter.TimeoutAnnotation)

      val tracer = new RawZipkinTracer("localhost", 1463, NullStatsReceiver, zipkinTimer)
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
