package com.twitter.finagle.benchmark

import com.twitter.finagle.Service
import com.twitter.finagle.tracing._
import com.twitter.util.Future
import com.google.caliper.SimpleBenchmark

class TracingFilterBenchmark extends SimpleBenchmark {
  val echoService = new Service[Int, Int] {
    def apply(req: Int) = Future.value(req)
  }
  val echoServiceWithFilter = new TracingFilter(NullTracer) andThen echoService

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

  def timeTracingFilter(n :Int) {
    test(n, echoServiceWithFilter)
  }

  def timeBareboneService(n: Int) {
    test(n, echoService)
  }
}