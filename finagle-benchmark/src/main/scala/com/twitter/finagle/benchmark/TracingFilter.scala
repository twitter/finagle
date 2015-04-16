package com.twitter.finagle.benchmark

import com.google.caliper.SimpleBenchmark
import com.twitter.finagle.Service
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.tracing._
import com.twitter.finagle.zipkin
import com.twitter.finagle.zipkin.thrift._
import com.twitter.finagle.zipkin.thriftscala.{LogEntry, ResultCode, Scribe}
import com.twitter.util.{Future, MockTimer}
import java.net.InetSocketAddress

// From $BIRDCAGE_HOME run:
// ./pants bench finagle/finagle-benchmark --bench-target=com.twitter.finagle.benchmark.TracingFilterBenchmark
class TracingFilterBenchmark extends SimpleBenchmark {
  val addr = new InetSocketAddress("127.0.0.1", 8080)

  val echoService = new Service[Int, Int] {
    def apply(req: Int) = {
      if (Trace.isActivelyTracing) {
        Trace.record(Annotation.ServerRecv())
        Trace.recordServiceName("testService")
        Trace.recordRpc("testRpc")
        Trace.record("this is a string message")
        Trace.record(Annotation.Message("frog blast the vent core"))
        Trace.recordBinaries(Map("fingle" -> 22, "dingle" -> 3.1415f))
        Trace.recordBinary("this is the key", "this is the value")
        Trace.recordServerAddr(addr)
      }
      val ret = Future.value(req)
      Trace.record(Annotation.ServerSend())
      ret
    }
  }

  val scribe = new Scribe.FutureIface {
    private[this] val res = Future.value(ResultCode.Ok)
    def log(msgs: Seq[LogEntry]): Future[ResultCode] = res
  }

  def mkTracer(on: Boolean): ZipkinTracer =
    new ZipkinTracer(
      RawZipkinTracer(scribe, NullStatsReceiver, new MockTimer),
      if (on) 1.0f else 0.0f)

  val echoServiceWithFilter = new TracingFilter(NullTracer, "echoService") andThen echoService

  // note the 0.0f sampleRate.  With zipkin tracer not sampling any requests, this should be as fast as
  // a bare service.
  val echoServiceWithZipkin = new TracingFilter(mkTracer(false), "echoService") andThen echoService

  val echoWithDoubleZipkinBroadcast = new TracingFilter(BroadcastTracer(Seq(mkTracer(false), mkTracer(false))), "echoService") andThen echoService

  val echoServiceWithZipkinOn = new TracingFilter(mkTracer(true), "echoService") andThen echoService

  def test(n: Int, service: Service[Int, Int]) {
    var i = 0
    while (i < n) {
      var j = 0
      while (j < 10000) {
        service(j)
        j += 1
      }
      i += 1
    }
  }

  def timeBroadcastDoubleZipkin(n: Int) {
    test(n, echoWithDoubleZipkinBroadcast)
  }

  def timeZipkinTracer(n: Int) {
    test(n, echoServiceWithZipkin)
  }

  def timeZipkinTracerOn(n: Int) {
    test(n, echoServiceWithZipkinOn)
  }

  def timeNullTracer(n: Int) {
    test(n, echoServiceWithFilter)
  }

  def timeBareboneService(n: Int) {
    test(n, echoService)
  }
}
