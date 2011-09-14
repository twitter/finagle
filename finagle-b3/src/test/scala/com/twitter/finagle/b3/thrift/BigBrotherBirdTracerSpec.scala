package com.twitter.finagle.b3.thrift

import org.specs.Specification
import org.specs.mock.Mockito
import java.util.ArrayList

import org.apache.scribe.{ResultCode, LogEntry, scribe}
import com.twitter.util._
import com.twitter.finagle.tracing._
import com.twitter.finagle.util.Timer
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver}

import org.mockito.Matchers._
import java.nio.ByteBuffer
import java.net.{Inet4Address, InetAddress, InetSocketAddress}

object BigBrotherBirdTracerSpec extends Specification with Mockito {

  val traceId = TraceId(Some(SpanId(123)), Some(SpanId(123)), SpanId(123), None)

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
        tracer.sampleTrace(TraceId(None, None, SpanId(i), None)) mustEqual Some(false)
      }
    }

    "drop none" in {
      val tracer = new BigBrotherBirdTracer(null, NullStatsReceiver)
      tracer.setSampleRate(1f)
      for (i <- 1 until 100) {
        tracer.sampleTrace(TraceId(None, None, SpanId(i), None)) mustEqual Some(true)
      }
    }

    "make a decision if sampled None" in {
      class TestTrace(client: scribe.ServiceToClient, statsReceiver: StatsReceiver = NullStatsReceiver)
        extends BigBrotherBirdTracer(client, statsReceiver) {
        var annotateCalled = false
        protected override def annotate(record: Record, value: String) = {
          annotateCalled = true
        }
      }
      val tracer1 = new TestTrace(null, NullStatsReceiver)
      tracer1.setSampleRate(1f)
      tracer1.record(new Record(traceId, Time.now, Annotation.ClientSend()))
      tracer1.annotateCalled mustEqual true
    }

    "send all traces to scribe" in {
      val client = mock[scribe.ServiceToClient]

      Timer.default.acquire()
      val tracer = new BigBrotherBirdTracer(client, NullStatsReceiver).setSampleRate(1)

      val expected = new ArrayList[LogEntry]()
      expected.add(new LogEntry().setCategory("b3")
        .setMessage("CgABAAAAAAAAAHsLAAMAAAAGbWV0aG9kCgAEAAAAAAAAAHsKAAUAAAAAAAAAew8ABgwAAAACCgAB" +
        "AAAAAAdU1MALAAIAAAACY3MMAAMIAAEBAQEBBgACAAELAAMAAAAHc2VydmljZQAACgABAAAAAAdU1MALAAIAAAAC" +
        "Y3IMAAMIAAEBAQEBBgACAAELAAMAAAAHc2VydmljZQAADQAHCwsAAAABAAAAA2tleQAAAAV2YWx1ZQA="))
      client.Log(anyObject()) returns Future(ResultCode.OK)

      val inetAddress = InetAddress.getByAddress(Array.fill(4) {1})
      tracer.record(Record(traceId, Time.fromSeconds(123),
        Annotation.ClientAddr(new InetSocketAddress(inetAddress, 1))))
      tracer.record(Record(traceId, Time.fromSeconds(123), Annotation.Rpcname("service", "method")))
      tracer.record(Record(traceId, Time.fromSeconds(123),
        Annotation.BinaryAnnotation("key", ByteBuffer.wrap("value".getBytes()))))
      tracer.record(Record(traceId, Time.fromSeconds(123), Annotation.ClientSend()))
      tracer.record(Record(traceId, Time.fromSeconds(123), Annotation.ClientRecv()))

      Timer.default.stop()
      there was one(client).Log(expected)
    }
  }
}