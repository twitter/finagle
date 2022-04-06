package com.twitter.finagle.loadbalancer

import com.twitter.finagle._
import com.twitter.finagle.benchmark.StdBenchAnnotations
import com.twitter.finagle.loadbalancer.LoadBalancerFactory.PanicMode
import com.twitter.finagle.stats.Counter
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.util.Activity
import com.twitter.util.Await
import com.twitter.util.Future
import com.twitter.util.Time
import com.twitter.util.Var
import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations._
import scala.collection.mutable.ArrayBuffer

object BalancerBench {

  val NoBrokersExc = new NoBrokersAvailableException

  def newFactory(): EndpointFactory[Unit, Unit] =
    new EndpointFactory[Unit, Unit] {
      val address: Address = Address.Failed(new Exception)
      def remake(): Unit = {}
      def apply(conn: ClientConnection): Future[Service[Unit, Unit]] =
        Future.value(new Service[Unit, Unit] {
          def apply(req: Unit): Future[Unit] = Future.Done
        })
      def close(when: Time): Future[Unit] = Future.Done
    }

  def newActivity(num: Int): Activity[Vector[EndpointFactory[Unit, Unit]]] = {
    val underlying = Var((0 until num).map(_ => newFactory()).toVector)
    Activity(underlying.map { facs => Activity.Ok(facs) })
  }

  private case class NullNode(factory: EndpointFactory[Unit, Unit])
      extends ServiceFactoryProxy[Unit, Unit](factory)
      with NodeT[Unit, Unit] {

    def load: Double = 0.0
    def pending: Int = 0
    override def close(deadline: Time): Future[Unit] = Future.Done
  }

  private case class NullDistributor(vec: Vector[NullNode]) extends DistributorT[NullNode](vec) {
    type This = NullDistributor
    def pick(): NullNode = vector.head
    def needsRebuild: Boolean = false
    def rebuild(): NullDistributor = this
    def rebuild(vector: Vector[NullNode]): NullDistributor = NullDistributor(vector)
  }

  private class NullBalancer extends Balancer[Unit, Unit] {
    private[loadbalancer] val panicMode: PanicMode = PanicMode.Paranoid

    protected def emptyException: Throwable = new Exception()
    protected def statsReceiver: StatsReceiver = NullStatsReceiver
    protected[this] def maxEffortExhausted: Counter =
      NullStatsReceiver.counter("")

    protected type Node = NullNode
    protected type Distributor = NullDistributor

    protected def newNode(factory: EndpointFactory[Unit, Unit]): Node =
      NullNode(factory)

    protected def initDistributor(): Distributor =
      NullDistributor(Vector.empty)

    def additionalMetadata: Map[String, Any] = Map.empty
  }

  @State(Scope.Benchmark)
  class UpdateState {

    private val input: Vector[Vector[EndpointFactory[Unit, Unit]]] = {
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

    def next(): Vector[EndpointFactory[Unit, Unit]] = {
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
  def setup(): Unit = {
    heap = Balancers
      .heap()
      .newBalancer(
        newActivity(numNodes),
        NoBrokersExc,
        Stack.Params.empty
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
  def setup(): Unit = {
    p2c = Balancers
      .p2c()
      .newBalancer(
        newActivity(numNodes),
        NoBrokersExc,
        Stack.Params.empty
      )
    p2cEwma = Balancers
      .p2cPeakEwma()
      .newBalancer(
        newActivity(numNodes),
        NoBrokersExc,
        Stack.Params.empty
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
  def setup(): Unit = {
    aperture = Balancers
      .aperture()
      .newBalancer(
        newActivity(numNodes),
        NoBrokersExc,
        Stack.Params.empty
      )
  }

  @Benchmark
  def getAndPut(): Unit = Await.result(aperture().flatMap(_.close()))
}
