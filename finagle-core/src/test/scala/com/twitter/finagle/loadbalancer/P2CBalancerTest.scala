package com.twitter.finagle.loadbalancer

import com.twitter.finagle._
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver, InMemoryStatsReceiver}
import com.twitter.finagle.util.Rng
import com.twitter.util._
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import scala.collection.mutable
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class P2CBalancerTest extends FunSuite {
  val N = 100
  val R = 100000
  val ε = 0.0001*R

  case class LoadedFactory(which: Int, weight: Double) extends ServiceFactory[Unit, Int] {
    var available = true
    var load = 0

    var sum = 0
    var count = 0

    // This isn't quite the right notion of mean load, but it's good enough.
    def meanLoad = if (count == 0) 0 else sum.toDouble/count.toDouble
    def normMeanLoad = if (weight==0) meanLoad else meanLoad/weight

    def apply(conn: ClientConnection) = {
      load += 1

      sum += load
      count += 1

      Future.value(new Service[Unit, Int] {
        def apply(req: Unit) = Future.value(which)
        override def close(deadline: Time) = {
          load -= 1
          sum += load
          count += 1
          Future.Done
        }
      })
    }

    def close(deadline: Time) = Future.Done
    override def toString = "LoadedFactory(%d)".format(load)
    override def isAvailable = available

    def tup: (ServiceFactory[Unit, Int], Double) = (this, weight)
  }
  
  def statsDict(r: InMemoryStatsReceiver) = new {
    private val zero = () => 0

    def size = r.gauges.getOrElse(Seq("size"), zero)()
    def adds = r.counters.getOrElse(Seq("adds"), 0)
    def removes = r.counters.getOrElse(Seq("removes"), 0)
    def load  = r.gauges.getOrElse(Seq("load"), zero)()
    def available  = r.gauges.getOrElse(Seq("available"), zero)()
    def meanweight  = r.gauges.getOrElse(Seq("meanweight"), zero)()
  }

  def newBal(fs: Var[Traversable[LoadedFactory]], 
      statsReceiver: StatsReceiver = NullStatsReceiver) =
    new P2CBalancer(fs map { fs => fs map (_.tup) }, 
      rng=Rng(12345L), statsReceiver=statsReceiver)

  def assertEven(fs: Traversable[LoadedFactory]) {
    val nml = fs.head.normMeanLoad
    for (f <- fs) {
      assert(math.abs(f.normMeanLoad - nml) < ε, 
        "nml=%f; f.nml=%f; ε=%f".format(nml, f.normMeanLoad, ε))
    }
  }

  test("Balances evenly when weights=i") {
    val init = Vector.tabulate(N) { i => new LoadedFactory(i, i+1) }
    val bal = newBal(Var.value(init))
    for (_ <- 0 until R) bal()
    assertEven(init)
  }

  test("Balances evenly when weights=1") {
    val init = Vector.tabulate(N) { i => new LoadedFactory(i, 1) }
    val bal = newBal(Var.value(init))
    for (_ <- 0 until R) bal()
    assertEven(init)
  }

  test("Balances evenly when weights=0") {
    val init = Vector.tabulate(N) { i => new LoadedFactory(i, 0) }
    val bal = newBal(Var.value(init))
    for (_ <- 0 until R) bal()
    assertEven(init)
  }

  test("Balance evenly when load varies") {
    val rng = Rng(12345L)
    val init = Vector.tabulate(N) { i => LoadedFactory(i, i+1) }
    var pending = Set[Service[Unit, Int]]()
    val bal = newBal(Var.value(init))

    for (i <- 0 until R) {
      i%3 match {
        case 0 =>
          pending += Await.result(bal())
        case 1 if rng.nextInt(2) == 0 =>
          pending += Await.result(bal())
        case _ if pending.nonEmpty =>
          val hd = pending.head
          pending -= hd
          hd.close()
        case _ =>
      }
    }
    
    assertEven(init)
  }
  
  test("Dynamically incorporates updates") {
    val init = Vector.tabulate(N) { i => LoadedFactory(i, 1) }
    val vec = Var(init)
    val bal = newBal(vec)

    for (_ <- 0 until R) bal()
    assertEven(vec())

    val fN1 = LoadedFactory(N+1, 2)
    vec() :+= fN1

    for (_ <- 0 until R*2) bal()
    assertEven(vec())

    // Spot check!
    assert(math.abs(init(0).load*2 - fN1.load) < ε)

    val init0Load = init(0).load
    vec() = vec() drop 1

    for (_ <- 0 until R) bal()
    assert(init0Load === init(0).load)
  }

  test("Skip downed nodes; revive them") {
    val init = Vector.tabulate(N) { i => new LoadedFactory(i, 1) }
    val bal = newBal(Var.value(init))

    var byIndex = new mutable.HashMap[Int, mutable.Set[Closable]] 
      with mutable.MultiMap[Int, Closable]

    def run(n: Int) {
      for (_ <- 0 until n) {
        val s = Await.result(bal())
        val i = Await.result(s(()))
        byIndex.addBinding(i, s)
      }
    }

    run(R)

    val init0Load = init(0).load
    init(0).available = false

    run(R)
    assert(init0Load === init(0).load)
    assertEven(init drop 1)

    Closable.all(byIndex(0).toSeq:_*).close()
    for (_ <- 0 until R) bal()
    assert(init(0).load === 0)
    assertEven(init drop 1)

    init(0).available = true

    run(R)

    // Because of 2-choices, we should see approximately
    // twice our normal load assignments in a period of R.
    // (This demonstrates nicely why and how P2C converges
    // slower than a heap-based balancer.)
    assert(math.abs(init(0).load - 2*R/N) < ε*5)
  }

  test("Handle empty vectors") {
    val vec = Var(Vector.empty[LoadedFactory])
    val bal = newBal(vec)
    intercept[NoBrokersAvailableException] { Await.result(bal()) }
    
    vec() :+= new LoadedFactory(0, 1)
    for (_ <- 0 until R) Await.result(bal())
    assert(vec().head.load === R)
    
    vec() = Vector.empty
    intercept[NoBrokersAvailableException] { Await.result(bal()) }
  }
  
  test("Balance all-downed nodes.") {
    val init = Vector.tabulate(N) { i => new LoadedFactory(i, 1) }
    val bal = newBal(Var.value(init))
    
    for (_ <- 0 until R) bal()
    assertEven(init)
    
    val init0Load = init(0).load
    for (f <- init) f.available = false
    for (_ <- 0 until R) Await.result(bal()) // make sure we don't throw

    assertEven(init)
    val init0Load2 = init(0).load
    assert(math.abs(init0Load*2 - init0Load2) < ε)
    
    for (f <- init drop N/2) f.available = true
    for (_ <- 0 until R) bal()
    
    assert(init0Load2 === init(0).load)
    assertEven(init drop N/2)
    assertEven(init take N/2)
  }
  
  test("Stats") {
    val statsReceiver = new InMemoryStatsReceiver
    val vec = Var(Vector.empty[LoadedFactory])
    val bal = newBal(vec, statsReceiver)
    val stats = statsDict(statsReceiver)

    assert(stats.load === 0)
    assert(stats.size === 0)
    assert(stats.adds === 0)
    assert(stats.removes === 0)
    assert(stats.available === 0)
    assert(stats.meanweight === 0)
    
    vec() +:= new LoadedFactory(0, 1)
    
    assert(stats.load === 0)
    assert(stats.size === 1)
    assert(stats.adds === 1)
    assert(stats.removes === 0)
    assert(stats.available === 1)
    assert(stats.meanweight === 1)

    vec() +:= new LoadedFactory(1, 2)

    assert(stats.load === 0)
    assert(stats.size === 2)
    assert(stats.adds === 2)
    assert(stats.removes === 0)
    assert(stats.available === 2)
    assert(stats.meanweight === 1.5)

    vec()(0).available = false
    assert(stats.available === 1)

    val svcs = Seq.fill(R) { Await.result(bal()) }
    assert(stats.load === R)
    assert(vec()(0).load === 0)
    assert(vec()(1).load === R)
    Closable.all(svcs:_*).close()
    assert(vec()(1).load === 0)
    assert(stats.load === 0)
  }
}
