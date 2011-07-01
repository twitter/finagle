package com.twitter.finagle.b3.thrift

import org.specs.Specification
import org.specs.mock.Mockito
import java.util.ArrayList

import org.apache.scribe.{ResultCode, LogEntry, scribe}
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.util._
import com.twitter.finagle.tracing._
import com.twitter.finagle.util.Timer

import org.mockito.Matchers._
import java.nio.ByteBuffer
import java.net.InetSocketAddress


object BigBrotherBirdTracerSpec extends Specification with Mockito {

  val traceId = TraceId(Some(SpanId(123)), Some(SpanId(123)), SpanId(123), false)

  "BigBrotherBirdReceiver" should {
    "throw exception if illegal sample rate" in {
      val tracer = new BigBrotherBirdTracer(null, NullStatsReceiver)
      tracer.setSampleRate(-1) must throwA[IllegalArgumentException]
      tracer.setSampleRate(1.1f) must throwA[IllegalArgumentException]
    }

    "drop all" in {
      val tracer = new BigBrotherBirdTracer(null, NullStatsReceiver)
      tracer.setSampleRate(0)
      for (i <- 1 until 100) {
        tracer.sampleTrace(TraceId(None, None, SpanId(i), false)) mustEqual true
      }
    }

    "drop none" in {
      val tracer = new BigBrotherBirdTracer(null, NullStatsReceiver)
      tracer.setSampleRate(1f)
      for (i <- 1 until 100) {
        tracer.sampleTrace(TraceId(None, None, SpanId(i), false)) mustEqual false
      }
    }

    "send all traces to scribe" in {
      val client = mock[scribe.ServiceToClient]

      Timer.default.acquire()
      val tracer = new BigBrotherBirdTracer(client, NullStatsReceiver).setSampleRate(0)

      val expected = new ArrayList[LogEntry]()
      expected.add(new LogEntry().setCategory("b3")
        .setMessage("CgABAAAAAAAAAHsLAAIAAAAHc2VydmljZQsAAwAAAAZtZXRob2QKAAQAAAAAAAAAewoABQ" +
        "AAAAAAAAB7DwAGDAAAAAIKAAEAAAAAB1TUwAsAAgAAAAJjcwwAAwgAAawQkCYGAAIAewAACgABAAAAAAdU" +
        "1MALAAIAAAACY3IMAAMIAAGsEJAmBgACAHsAAA0ABwsLAAAAAQAAAANrZXkAAAAFdmFsdWUA"))
      client.Log(anyObject()) returns Future(ResultCode.OK)

      tracer.record(Record(traceId, Time.fromSeconds(123), Annotation.Rpcname("service", "method")))
      tracer.record(Record(traceId, Time.fromSeconds(123), Annotation.ClientAddr(new InetSocketAddress(123))))
      tracer.record(Record(traceId, Time.fromSeconds(123),
        Annotation.BinaryAnnotation("key", ByteBuffer.wrap("value".getBytes()))))
      tracer.record(Record(traceId, Time.fromSeconds(123), Annotation.ClientSend()))
      tracer.record(Record(traceId, Time.fromSeconds(123), Annotation.ClientRecv()))

      Timer.default.stop()
      there was one(client).Log(expected)
    }
  }
}