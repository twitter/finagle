package com.twitter.finagle.b3.thrift

import org.specs.Specification
import org.specs.mock.Mockito
import java.util.ArrayList

import com.twitter.util._
import com.twitter.finagle.tracing._
import com.twitter.finagle.util.{Timer, CloseNotifier}
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver}

import org.mockito.Matchers._
import java.nio.ByteBuffer
import java.net.{InetAddress, InetSocketAddress}
import com.twitter.finagle.service.TimeoutFilter

object BigBrotherBirdTracerSpec extends Specification with Mockito {

  val traceId = TraceId(Some(SpanId(123)), Some(SpanId(123)), SpanId(123), None)

  "BigBrotherBirdReceiver" should {
    "throw exception if illegal sample rate" in {
      val tracer = new BigBrotherBirdTracer("localhost", 1463, NullStatsReceiver, 0)
      tracer.setSampleRate(-1) must throwA[IllegalArgumentException]
      tracer.setSampleRate(1.1f) must throwA[IllegalArgumentException]
    }

    "drop all" in {
      val tracer = new BigBrotherBirdTracer("localhost", 1463, NullStatsReceiver, 0)
      tracer.setSampleRate(0)
      for (i <- 1 until 100) {
        tracer.sampleTrace(TraceId(None, None, SpanId(i), None)) mustEqual Some(false)
      }
    }

    "drop none" in {
      val tracer = new BigBrotherBirdTracer("localhost", 1463, NullStatsReceiver, 0)
      tracer.setSampleRate(1f)
      for (i <- 1 until 100) {
        tracer.sampleTrace(TraceId(None, None, SpanId(i), None)) mustEqual Some(true)
      }
    }

    "make a decision if sampled None" in {
      class TestTrace(statsReceiver: StatsReceiver = NullStatsReceiver)
        extends BigBrotherBirdTracer("localhost", 1463, statsReceiver, 0) {
        var annotateCalled = false
        protected override def annotate(record: Record, value: String) = {
          annotateCalled = true
        }
      }
      val tracer1 = new TestTrace(NullStatsReceiver)
      tracer1.setSampleRate(1f)
      tracer1.record(new Record(traceId, Time.now, Annotation.ClientSend()))
      tracer1.annotateCalled mustEqual true
    }

    "send all traces to scribe" in {
      Timer.default.acquire()
      val tracer = new BigBrotherBirdTracer("localhost", 1463, NullStatsReceiver, 0).setSampleRate(1)
      tracer.client = mock[scribe.ServiceToClient]

      val expected = new ArrayList[LogEntry]()
      expected.add(new LogEntry().setCategory("b3")
        .setMessage("CgABAAAAAAAAAHsLAAMAAAAGbWV0aG9kCgAEAAAAAAAAAHsKAAUAAAAAAAAAew8ABgwAA" +
        "AACCgABAAAAAAdU1MALAAIAAAACY3MMAAMIAAEBAQEBBgACAAELAAMAAAAHc2VydmljZQAACgABAAAAAA" +
        "dU1MALAAIAAAACY3IMAAMIAAEBAQEBBgACAAELAAMAAAAHc2VydmljZQAADwAIDAAAAAELAAEAAAADa2V" +
        "5CwACAAAABXZhbHVlCAADAAAAAQwABAgAAQEBAQEGAAIAAQsAAwAAAAdzZXJ2aWNlAAAA"))
      tracer.client.Log(anyObject()) returns Future(ResultCode.OK)

      val inetAddress = InetAddress.getByAddress(Array.fill(4) {1})
      tracer.record(Record(traceId, Time.fromSeconds(123),
        Annotation.ClientAddr(new InetSocketAddress(inetAddress, 1))))
      tracer.record(Record(traceId, Time.fromSeconds(123), Annotation.Rpcname("service", "method")))
      tracer.record(Record(traceId, Time.fromSeconds(123),
        Annotation.BinaryAnnotation("key", ByteBuffer.wrap("value".getBytes()))))
      tracer.record(Record(traceId, Time.fromSeconds(123), Annotation.ClientSend()))
      tracer.record(Record(traceId, Time.fromSeconds(123), Annotation.ClientRecv()))

      Timer.default.stop()
      there was one(tracer.client).Log(expected)
    }

    "register onClose handler that calls release" in {
      var callback: () => Unit = null
      val factory = BigBrotherBirdTracer()
      val tracer = factory(new CloseNotifier { def onClose(h: => Unit) = callback = () => h })
        .asInstanceOf[BigBrotherBirdTracer]

      tracer.client mustNot beNull
      callback()
      // tracer.release() should set client to null
      tracer.client must beNull
    }

    "logSpan if a timeout occurs" in {
      val ann1 = Annotation.Message("some_message")
      val ann2 = Annotation.Rpcname("some_service", "rpc_name")
      val ann3 = Annotation.Message(TimeoutFilter.TimeoutAnnotation)

      val tracer = new BigBrotherBirdTracer("localhost", 1463, NullStatsReceiver, 1)
      tracer.client = mock[scribe.ServiceToClient]

      tracer.client.Log(anyObject()) returns Future(ResultCode.OK)

      Timer.default.acquire()
      tracer.record(Record(traceId, Time.fromSeconds(1), ann1))
      tracer.record(Record(traceId, Time.fromSeconds(2), ann2))
      tracer.record(Record(traceId, Time.fromSeconds(3), ann3))

      // scribe Log method is in java
      there was one(tracer.client).Log(any[java.util.List[LogEntry]])
    }
  }
}
