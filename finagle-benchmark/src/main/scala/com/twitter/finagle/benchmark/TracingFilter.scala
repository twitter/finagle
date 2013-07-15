package com.twitter.finagle.benchmark

import com.twitter.finagle.Service
import com.twitter.finagle.tracing._
import com.twitter.util.Future
import com.twitter.finagle.zipkin
import com.google.caliper.SimpleBenchmark
import com.twitter.finagle.zipkin.thrift.ZipkinTracer
import com.twitter.finagle.tracing.Annotation.{ServerSend, ServerRecv, Message, ClientSend}
import java.net.InetSocketAddress

// From $BIRDCAGE_HOME run:
// ./bin/caliper finagle/finagle-benchmark com.twitter.finagle.benchmark.TracingFilterBenchmark
class TracingFilterBenchmark extends SimpleBenchmark {
  val addr = new InetSocketAddress("127.0.0.1", 8080)

  val echoService = new Service[Int, Int] {
    def apply(req: Int) = {
      if (Trace.isActivelyTracing) {
        Trace.record(ServerRecv())
        Trace.recordRpcname("testService", "testRpc")
        Trace.record("this is a string message")
        Trace.record(Message("frog blast the vent core"))
        Trace.recordBinaries(Map("fingle" -> 22, "dingle" -> 3.1415f))
        Trace.recordBinary("this is the key", "this is the value")
        Trace.recordServerAddr(addr)
      }
      val ret = Future.value(req)
      Trace.record(ServerSend())
      ret
    }
  }
  val echoServiceWithFilter = new TracingFilter(NullTracer) andThen echoService

  // note the 0.0f sampleRate.  With zipkin tracer not sampling any requests, this should be as fast as
  // a bare service.
  val echoServiceWithZipkin = new TracingFilter(ZipkinTracer.mk(sampleRate=0.0f)) andThen echoService


  val echoWithDoubleZipkinBroadcast = new TracingFilter(BroadcastTracer(Seq(ZipkinTracer.mk(sampleRate=0.0f), ZipkinTracer.mk(sampleRate=0.0f)))) andThen echoService

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

  def timeNullTracer(n: Int) {
    test(n, echoServiceWithFilter)
  }

  def timeBareboneService(n: Int) {
    test(n, echoService)
  }
}