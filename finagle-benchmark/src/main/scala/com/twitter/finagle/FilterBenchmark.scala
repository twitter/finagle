package com.twitter.finagle

import com.twitter.finagle.benchmark.StdBenchAnnotations
import com.twitter.util.Future
import org.openjdk.jmh.annotations._

// ./sbt 'project finagle-benchmark' 'run .*FilterBenchmark.*'
@State(Scope.Benchmark)
class FilterBenchmark extends StdBenchAnnotations {
  import FilterBenchmark._

  @Param(Array("10"))
  var numAndThens: Int = _

  val mutable = new Mutable(0)

  var svc: Service[Mutable, Mutable] = _

  @Setup
  def createSvc(): Unit = {
    val filter = new SimpleFilter[Mutable, Mutable] {
      def apply(req: Mutable, next: Service[Mutable, Mutable]): Future[Mutable] = {
        req.count += 1
        next(req)
      }
    }

    var f: Filter[Mutable, Mutable, Mutable, Mutable] = filter
    for (i <- 0.until(numAndThens)) {
      f = f.andThen(filter)
    }
    svc = f.andThen(Service.const(Future.value(mutable)))
  }

  @Benchmark
  def andThenFilter(): Future[Mutable] = {
    svc(mutable)
  }
}

object FilterBenchmark {
  class Mutable(var count: Int)
}
