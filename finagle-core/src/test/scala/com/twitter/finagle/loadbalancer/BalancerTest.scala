package com.twitter.finagle.loadbalancer

import com.twitter.finagle._
import com.twitter.finagle.loadbalancer._
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver}
import com.twitter.util.{Future, Time}
import java.util.concurrent.atomic.AtomicInteger
import org.junit.runner.RunWith
import org.scalacheck.{Arbitrary, Gen}
import org.scalactic.Tolerance
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.concurrent.Conductors
import org.scalatest.prop.GeneratorDrivenPropertyChecks

@RunWith(classOf[JUnitRunner])
private class BalancerTest extends FunSuite with Conductors with GeneratorDrivenPropertyChecks {

  private class TestBalancer extends Balancer[Unit, Unit] {
    def maxEffort: Int = ???
    def emptyException: Throwable = ???
    
    protected def statsReceiver: StatsReceiver = NullStatsReceiver
    
    def nodes: Vector[Node] = dist.vector
    def factories: Set[ServiceFactory[Unit, Unit]] = nodes.map(_.factory).toSet
    
    def nodeOf(fac: ServiceFactory[Unit, Unit]) = nodes.find(_.factory == fac).get

    def _dist() = dist
    def _rebuild() = rebuild()
    
    def rebuildDistributor() {}

    case class Distributor(vector: Vector[Node], gen: Int = 1) extends DistributorT {
      type This = Distributor
      def pick(): Node = ???
      def needsRebuild = ???
      def rebuild(): This = {
        rebuildDistributor()
        copy(gen=gen+1)
      }
      def rebuild(vector: Vector[Node]): This = {
        rebuildDistributor()
        copy(vector, gen=gen+1)
      }
    }

    class Node(val factory: ServiceFactory[Unit, Unit], val weight: Double) extends NodeT {
      type This = Node
      def load: Double = ???
      def pending: Int = ???
      def token: Int = ???
      def newWeight(weight: Double): This = new Node(factory, weight)
      def close(deadline: Time): Future[Unit] = TestBalancer.this.synchronized {
        factory.close()
        Future.Done
      }
      def apply(conn: ClientConnection): Future[Service[Unit,Unit]] = ???
    }

    protected def newNode(factory: ServiceFactory[Unit, Unit], 
      weight: Double, statsReceiver: StatsReceiver): Node = 
      new Node(factory, weight)

    protected def failingNode(cause: Throwable): Node = ???
    
    protected def initDistributor(): Distributor = Distributor(Vector.empty)
  }
  
  def newFac(_status: Status = Status.Open) = new ServiceFactory[Unit, Unit] {
    def apply(conn: ClientConnection) = Future.never

    override def status = _status
    
    @volatile var ncloses = 0

    def close(deadline: Time) = {
      synchronized { ncloses += 1 }
      Future.Done
    }
  }

  val genStatus = Gen.oneOf(Status.Open, Status.Busy, Status.Closed)
  val genSvcFac = genStatus.map(newFac)
  val genLoadedNode =
    for {
      fac  <- genSvcFac
      load <- Gen.chooseNum(Int.MinValue, Int.MaxValue).map(_.toDouble)
    } yield fac -> load

  val genNodes = Gen.containerOf[List,(ServiceFactory[Unit,Unit],Double)](genLoadedNode)

  test("status: balancer with no nodes is Closed") {
    val bal = new TestBalancer
    assert(bal.nodes.isEmpty)
    assert(bal.status === Status.Closed)
  }

  test("status: balancer status is bestOf underlying nodes") {
    forAll(genNodes) { loadedNodes =>
      val bal = new TestBalancer
      bal.update(loadedNodes)
      val best = Status.bestOf[ServiceFactory[Unit,Unit]](loadedNodes.map(_._1), _.status)
      assert(bal.status === best)
    }
  }

  test("status: balancer with at least one Open node is Open") {
    val bal = new TestBalancer
    val f1, f2 = newFac(Status.Closed)
    val f3 = newFac(Status.Open)
    bal.update(Seq(f1->1, f2->1, f3->1))

    assert(bal.status === Status.Open)

    // all closed
    bal.update(Seq(f1->1, f2->1))
    assert(bal.status === Status.Closed)

    // one busy, remainder closed
    val busy = newFac(Status.Busy)
    bal.update(Seq(f1->1, f2->1, busy->1))
    assert(bal.status === Status.Busy)
  }


  test("updater: keeps nodes up to date") {
    val bal = new TestBalancer
    val f1, f2, f3 = newFac()

    assert(bal.nodes.isEmpty)
    bal.update(Seq(f1->1, f2->1, f3->1))
    assert(bal.factories === Set(f1, f2, f3))
    
    for (f <- Seq(f1, f2, f3))
      assert(f.ncloses === 0)

    bal.update(Seq(f1->1, f3->1))

    assert(bal.factories === Set(f1, f3))
    assert(f1.ncloses === 0)
    assert(f2.ncloses === 1)
    assert(f3.ncloses === 0)
    
    // Updates weights.
    bal.update(Seq(f1->1, f3->2))
    assert(bal.nodeOf(f1).weight === 1)
    assert(bal.nodeOf(f3).weight === 2)
  }
  
  test("Coalesces updates") {
    val conductor = new Conductor
    import conductor._

    val bal = new TestBalancer {
      val beat = new AtomicInteger(1)
      @volatile var updateThreads: Set[Long] = Set.empty

      override def rebuildDistributor() {
        synchronized { updateThreads += Thread.currentThread.getId() }
        waitForBeat(beat.getAndIncrement())
        waitForBeat(beat.getAndIncrement())
      }
    }
    val f1, f2, f3 = newFac()
    
    @volatile var thread1Id: Long = -1 
    
    thread("updater1") {
      thread1Id = Thread.currentThread.getId()
      bal.update(Seq.empty) // waits for 1, 2
      // (then waits for 3, 4, in this thread)
    }

    thread("updater2") {
      waitForBeat(1)
      bal._rebuild()
      bal.update(Seq(f1->1))
      bal.update(Seq(f2->1))
      bal._rebuild()
      bal.update(Seq(f3->1))
      bal._rebuild()
      assert(beat === 1)
      waitForBeat(2)
    }
    
    whenFinished {
      assert(bal.factories === Set(f3))
      assert(bal._dist.gen === 3)
      assert(bal.updateThreads === Set(thread1Id))
    }
  }
}
