package com.twitter.finagle.loadbalancer

import com.twitter.finagle.benchmark.StdBenchAnnotations
import com.twitter.finagle.NoBrokersAvailableException
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.{Service, ServiceFactory}
import com.twitter.util.{Await, Activity, Future, Var}
import org.openjdk.jmh.annotations._

object BalancerBench {
  val NoBrokersExc = new NoBrokersAvailableException

  def newFactory(): ServiceFactory[Unit, Unit] =
    ServiceFactory.const(new Service[Unit, Unit] {
      def apply(req: Unit) = Future.Done
    })

  def newActivity(num: Int): Activity[Set[ServiceFactory[Unit, Unit]]] = {
    val underlying = Var((0 until num).map(_ => newFactory()).toSet)
    Activity(underlying.map { facs => Activity.Ok(facs) })
  }
}

@State(Scope.Benchmark)
@Threads(Threads.MAX)
class HeapBalancerBench extends StdBenchAnnotations {
  import BalancerBench._

  @Param(Array("1000"))
  var numNodes: Int = _

  var heap: ServiceFactory[Unit, Unit] = _

  @Setup
  def setup() {
    heap = Balancers.heap().newBalancer(
      newActivity(numNodes), NullStatsReceiver, NoBrokersExc
    )
  }

  @Benchmark
  def getAndPut(): Unit = Await.result(heap().flatMap(_.close()))
}

@State(Scope.Benchmark)
@Threads(Threads.MAX)
class P2CBalancerBench extends StdBenchAnnotations {
  import BalancerBench._

  @Param(Array("1000"))
  var numNodes: Int = _

  var p2c: ServiceFactory[Unit, Unit] = _
  var p2cEwma: ServiceFactory[Unit, Unit] = _

  @Setup
  def setup() {
    p2c = Balancers.p2c().newBalancer(
      newActivity(numNodes), NullStatsReceiver, NoBrokersExc
    )
    p2cEwma = Balancers.p2cPeakEwma().newBalancer(
      newActivity(numNodes), NullStatsReceiver, NoBrokersExc
    )
  }

  @Benchmark
  def leastLoadedGetAndPut(): Unit = Await.result(p2c().flatMap(_.close()))

  @Benchmark
  def ewmaGetAndPut(): Unit = Await.result(p2cEwma().flatMap(_.close()))
}

@State(Scope.Benchmark)
@Threads(Threads.MAX)
class ApertureBalancerBench extends StdBenchAnnotations {
  import BalancerBench._

  @Param(Array("1000"))
  var numNodes: Int = _

  var aperture: ServiceFactory[Unit, Unit] = _

  @Setup
  def setup() {
    aperture = Balancers.aperture().newBalancer(
      newActivity(numNodes), NullStatsReceiver, NoBrokersExc
    )
  }

  @Benchmark
  def getAndPut(): Unit = Await.result(aperture().flatMap(_.close()))
}
