package com.twitter.finagle.loadbalancer

import com.twitter.finagle.benchmark.StdBenchAnnotations
import com.twitter.finagle.{NoBrokersAvailableException, Service, ServiceFactory, ServiceFactoryProxy}
import com.twitter.finagle.stats.{Counter, NullStatsReceiver, StatsReceiver}
import com.twitter.util.{Activity, Await, Future, Time, Var}
import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations._
import scala.collection.mutable.ArrayBuffer

object BalancerBench {

  val NoBrokersExc = new NoBrokersAvailableException

  def newFactory(): ServiceFactory[Unit, Unit] =
    ServiceFactory.const(new Service[Unit, Unit] {
      def apply(req: Unit): Future[Unit] = Future.Done
    })

  def newActivity(num: Int): Activity[Vector[ServiceFactory[Unit, Unit]]] = {
    val underlying = Var((0 until num).map(_ => newFactory()).toVector)
    Activity(underlying.map { facs => Activity.Ok(facs) })
  }

  case class NullNode(factory: ServiceFactory[Unit, Unit])
    extends ServiceFactoryProxy[Unit, Unit](factory) with NodeT[Unit, Unit] {

    override def load: Double = 0.0
    override def pending: Int = 0
    override def token: Int = 0
    override def close(deadline: Time): Future[Unit] = Future.Done
  }

  case class NullDistibutor(vec: Vector[NullNode])
    extends DistributorT[NullNode](vec) {
    override type This = NullDistibutor
    override def pick(): NullNode = vector.head
    override def needsRebuild: Boolean = false
    override def rebuild(): NullDistibutor = this
    override def rebuild(vector: Vector[NullNode]): NullDistibutor = NullDistibutor(vector)
  }

  private class NullBalancer extends Balancer[Unit, Unit] {
    override protected def maxEffort: Int = 0
    override protected def emptyException: Throwable = new Exception()
    override protected def statsReceiver: StatsReceiver = NullStatsReceiver
    override protected[this] def maxEffortExhausted: Counter =
      NullStatsReceiver.counter0("")

    override protected type Node = NullNode
    override protected type Distributor = NullDistibutor

    override protected def newNode(
      factory: ServiceFactory[Unit, Unit],
      statsReceiver: StatsReceiver
    ): Node = NullNode(factory)

    override protected def failingNode(cause: Throwable): Node =
      NullNode(ServiceFactory.const(Service.const(Future.exception(cause))))

    override protected def initDistributor(): Distributor =
      NullDistibutor(Vector.empty)
  }

  @State(Scope.Benchmark)
  class UpdateState {

    private val input: Vector[Vector[ServiceFactory[Unit, Unit]]] = {
      val prev = Vector.fill(5000)(BalancerBench.newFactory())
      val result = ArrayBuffer.apply(prev)

      for (i <- 0 until 10) {
        val slice = Vector.fill(500)(BalancerBench.newFactory())
        val next = prev.take(i * 500) ++ slice ++ prev.drop((i + 1) * 500)
        result += next
      }

      result.toVector
    }

    private var index: Int = 0

    def next(): Vector[ServiceFactory[Unit, Unit]] = {
      val n = input(index)
      index = (index + 1) % input.length
      n
    }
  }
}

@State(Scope.Benchmark)
@Fork(2)
@Warmup(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
class BalancerBench extends StdBenchAnnotations {
  import BalancerBench._

  private[this] val noBalancer: NullBalancer = new NullBalancer

  @Benchmark
  def update5000x500(state: UpdateState): Unit = {
    noBalancer.update(state.next())
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
