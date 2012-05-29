package com.twitter.finagle.b3.thrift

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

object RawBigBrotherBirdTracerSpec extends Specification with Mockito {

  val traceId = TraceId(Some(SpanId(123)), Some(SpanId(123)), SpanId(123), None)

  "RawBigBrotherBirdTracer" should {
    val b3Timer = FinagleTimer.getManaged.make()
    doAfter { b3Timer.dispose() }

    "send all traces to scribe" in {
      val tracer = new RawBigBrotherBirdTracer("localhost", 1463, NullStatsReceiver, b3Timer)
      tracer.client = mock[scribe.ServiceToClient]

      val expected = new ArrayList[LogEntry]()
      expected.add(new LogEntry().setCategory("b3")
        .setMessage("CgABAAAAAAAAAHsLAAMAAAAGbWV0aG9kCgAEAAAAAAAAAHsKAAUAAAAAAAAAew8ABgwAA" +
        "AACCgABAAAAAAdU1MALAAIAAAACY3MMAAMIAAEBAQEBBgACAAELAAMAAAAHc2VydmljZQAACgABAAAAAA" +
        "dU1MALAAIAAAACY3IMAAMIAAEBAQEBBgACAAELAAMAAAAHc2VydmljZQAADwAIDAAAAAELAAEAAAADa2V" +
        "5CwACAAAABXZhbHVlCAADAAAAAQwABAgAAQEBAQEGAAIAAQsAAwAAAAdzZXJ2aWNlAAAA"))
      tracer.client.Log(anyObject()) returns Future(ResultCode.OK)

      val inetAddress = InetAddress.getByAddress(Array.fill(4) {
        1
      })
      tracer.record(Record(traceId, Time.fromSeconds(123),
        Annotation.ClientAddr(new InetSocketAddress(inetAddress, 1))))
      tracer.record(Record(traceId, Time.fromSeconds(123), Annotation.Rpcname("service", "method")))
      tracer.record(Record(traceId, Time.fromSeconds(123),
        Annotation.BinaryAnnotation("key", ByteBuffer.wrap("value".getBytes()))))
      tracer.record(Record(traceId, Time.fromSeconds(123), Annotation.ClientSend()))
      tracer.record(Record(traceId, Time.fromSeconds(123), Annotation.ClientRecv()))

      there was one(tracer.client).Log(expected)
    }

    "register onClose handler that calls release" in {
      var callback: () => Unit = null
      val factory = RawBigBrotherBirdTracer()
      val tracer = factory(new CloseNotifier {
        def onClose(h: => Unit) = callback = () => h
      }).asInstanceOf[RawBigBrotherBirdTracer]

      tracer.client mustNot beNull
      callback()
      // tracer.release() should set client to null
      tracer.client must beNull
    }

    "logSpan if a timeout occurs" in {
      val ann1 = Annotation.Message("some_message")
      val ann2 = Annotation.Rpcname("some_service", "rpc_name")
      val ann3 = Annotation.Message(TimeoutFilter.TimeoutAnnotation)

      val tracer = new RawBigBrotherBirdTracer("localhost", 1463, NullStatsReceiver, b3Timer)
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
