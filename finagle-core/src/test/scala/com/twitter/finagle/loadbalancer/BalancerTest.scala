package com.twitter.finagle.loadbalancer

import com.twitter.conversions.DurationOps._
import com.twitter.finagle._
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util.Await
import com.twitter.util.Future
import com.twitter.util.Time
import org.scalacheck.Gen
import org.scalatest.concurrent.Conductors
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import scala.language.reflectiveCalls

class BalancerTest extends AnyFunSuite with Conductors with ScalaCheckDrivenPropertyChecks {

  private class TestBalancer(
    protected val statsReceiver: InMemoryStatsReceiver = new InMemoryStatsReceiver,
    singletonDistributor: Boolean = false)
      extends Balancer[Unit, Unit] {

    def panicMode: PanicMode = PanicMode.MajorityUnhealthy
    def emptyException: Throwable = ???

    def additionalMetadata: Map[String, Any] = Map.empty

    def stats: InMemoryStatsReceiver = statsReceiver

    def nodes: Vector[Node] = dist.vector
    def factories: Set[ServiceFactory[Unit, Unit]] = nodes.map(_.factory).toSet

    def nodeOf(fac: ServiceFactory[Unit, Unit]): Node = nodes.find(_.factory == fac).get

    def _dist(): Distributor = dist
    def _rebuild(): Unit = rebuild()

    def rebuildDistributor(): Unit = {}

    case class Distributor(vec: Vector[Node], singleton: Boolean, gen: Int = 1)
        extends DistributorT[Node](vec) {
      type This = Distributor
      def pick(): Node = vector.head
      def rebuild(): This =
        if (singleton) this
        else rebuild(vec)

      def rebuild(vector: Vector[Node]): This = {
        rebuildDistributor()
        copy(vector, gen = gen + 1)
      }
      def needsRebuild: Boolean = false
    }

    class Node(val factory: EndpointFactory[Unit, Unit])
        extends ServiceFactoryProxy(factory)
        with NodeT[Unit, Unit] {
      def load: Double = ???
      override def close(deadline: Time): Future[Unit] = TestBalancer.this.synchronized {
        factory.close()
        Future.Done
      }
      override def apply(conn: ClientConnection): Future[Service[Unit, Unit]] = Future.never

      override def toString: String =
        f"Node(load=NotImplemented, pending=NotImplemented, factory=$factory)"
    }

    protected def newNode(factory: EndpointFactory[Unit, Unit]): Node = new Node(factory)

    protected def initDistributor(): Distributor = Distributor(Vector.empty, singletonDistributor)
  }

  def newFac(_status: Status = Status.Open) = new EndpointFactory[Unit, Unit] {
    val address: Address = Address.Failed(new Exception)
    def remake(): Unit = ()

    def apply(conn: ClientConnection): Future[Service[Unit, Unit]] = Future.never
    override def status: Status = _status

    @volatile var ncloses = 0

    def close(deadline: Time): Future[Unit] = {
      synchronized { ncloses += 1 }
      Future.Done
    }
  }

  private val genStatus = Gen.oneOf(Status.Open, Status.Busy, Status.Closed)
  private val genSvcFac = genStatus.map(newFac)
  private val genLoadedNode = for (fac <- genSvcFac) yield fac
  private val genNodes = Gen.containerOf[Vector, EndpointFactory[Unit, Unit]](genLoadedNode)

  test("status: balancer with no nodes is Closed") {
    val bal = new TestBalancer
    assert(bal.nodes.isEmpty)
    assert(bal.status == Status.Closed)
  }

  test("status: balancer status is bestOf underlying nodes") {
    forAll(genNodes) { loadedNodes =>
      val bal = new TestBalancer
      bal.update(loadedNodes)
      val best = Status.bestOf[ServiceFactory[Unit, Unit]](loadedNodes, _.status)
      assert(bal.status == best)
    }
  }

  test("status: balancer with at least one Open node is Open") {
    val bal = new TestBalancer
    val f1, f2 = newFac(Status.Closed)
    val f3 = newFac(Status.Open)
    bal.update(Vector(f1, f2, f3))

    assert(bal.status == Status.Open)

    // all closed
    bal.update(Vector(f1, f2))
    assert(bal.status == Status.Closed)

    // one busy, remainder closed
    val busy = newFac(Status.Busy)
    bal.update(Vector(f1, f2, busy))
    assert(bal.status == Status.Busy)
  }

  test("panicked counter updated properly") {
    val bal = new TestBalancer()
    val closed = newFac(Status.Closed)
    val open = newFac(Status.Open)

    // start out all closed
    bal.update(Vector(closed))
    bal(ClientConnection.nil)
    assert(1 == bal.stats.counters(Seq("panicked")))

    // now have it be open and a pick must succeed
    bal.update(Vector(open))
    bal(ClientConnection.nil)
    assert(1 == bal.stats.counters(Seq("panicked")))
  }

  test("panicked: rebuilds increments for a new distributor") {
    val bal = new TestBalancer()
    val closed = newFac(Status.Closed)
    assert(bal._dist().gen == 1)

    bal.update(Vector(closed))
    assert(bal._dist().gen == 2)
    assert(1 == bal.stats.counters(Seq("rebuilds")))

    bal(ClientConnection.nil)
    assert(bal._dist().gen == 3)
    assert(1 == bal.stats.counters(Seq("panicked")))
    assert(2 == bal.stats.counters(Seq("rebuilds")))
  }

  test("panicked: accounts for no-op rebuild") {
    val bal = new TestBalancer(singletonDistributor = true)
    val closed = newFac(Status.Closed)
    assert(bal._dist().gen == 1)

    bal.update(Vector(closed))
    assert(bal._dist().gen == 2)
    assert(1 == bal.stats.counters(Seq("rebuilds")))

    bal(ClientConnection.nil)
    assert(bal._dist().gen == 2)
    assert(1 == bal.stats.counters(Seq("panicked")))
    assert(1 == bal.stats.counters(Seq("rebuilds")))
  }

  test("updater: keeps nodes up to date") {
    val bal = new TestBalancer
    val f1, f2, f3 = newFac()

    val adds = bal.stats.counter("adds")
    val rems = bal.stats.counter("removes")
    val size = bal.stats.gauges(Seq("size"))
    assert(bal.nodes.isEmpty)
    assert(size() == 0)
    assert(adds() == 0)
    assert(rems() == 0)

    bal.update(Vector(f1, f2, f3))
    assert(bal.factories == Set(f1, f2, f3))
    assert(size() == 3)
    assert(adds() == 3)
    assert(rems() == 0)
    for (f <- Seq(f1, f2, f3))
      assert(f.ncloses == 0)

    bal.update(Vector(f1, f3))
    assert(size() == 2)
    assert(adds() == 3)
    assert(rems() == 1)
    assert(bal.factories == Set(f1, f3))
    assert(f1.ncloses == 0)
    assert(f2.ncloses == 0)
    assert(f3.ncloses == 0)

    bal.update(Vector(f1, f2, f3))
    assert(bal.factories == Set(f1, f2, f3))
    assert(size() == 3)
    assert(adds() == 4)
    assert(rems() == 1)
  }

  test("prevent rebuilds on updates with no changes") {
    val bal = new TestBalancer
    val f1, f2 = newFac()

    bal.update(Vector(f1, f2))
    assert(bal.stats.counters(Seq("rebuilds")) == 1)
    assert(bal.stats.counters(Seq("updates")) == 1)

    bal.update(Vector(f2, f1))
    assert(bal.stats.counters(Seq("rebuilds")) == 1)
    assert(bal.stats.counters(Seq("updates")) == 2)

    val f3 = newFac()
    bal.update(Vector(f1, f2, f3))
    assert(bal.stats.counters(Seq("rebuilds")) == 2)
    assert(bal.stats.counters(Seq("updates")) == 3)
  }

  test("update order and element caching") {
    val bal = new TestBalancer
    val f1, f2, f3 = newFac()

    val update0 = Vector(f1, f2, f3)
    bal.update(update0)
    assert(bal._dist().vector.indices.forall { i => update0(i) eq bal._dist().vector(i).factory })

    val update1 = Vector(f1, f2, f3, newFac(), newFac())
    bal.update(update1)

    assert(bal._dist().vector(0).factory eq f1)
    assert(bal._dist().vector(1).factory eq f2)
    assert(bal._dist().vector(2).factory eq f3)
    assert(bal._dist().vector.indices.forall { i => update1(i) eq bal._dist().vector(i).factory })
  }

  test("close unregisters from the registry") {
    val label = "test_balancer_" + System.currentTimeMillis
    val bal = new TestBalancer()
    val registry = BalancerRegistry.get
    registry.register(label, bal)
    assert(registry.allMetadata.exists(_.label == label))

    Await.result(bal.close(), 5.seconds)
    assert(!registry.allMetadata.exists(_.label == label))
  }
}
