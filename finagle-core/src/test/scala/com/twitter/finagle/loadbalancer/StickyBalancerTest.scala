package com.twitter.finagle.loadbalancer

import com.twitter.app.App
import com.twitter.conversions.time._
import com.twitter.finagle._
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver, InMemoryStatsReceiver}
import com.twitter.util.{Function => _, _}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import scala.collection.mutable

private[loadbalancer] trait StickySuite {
  // number of servers
  val N: Int = 100
  // number of reqs
  val R: Int = 100000

  trait SServiceFactory extends ServiceFactory[Unit, Int]

  val noBrokers = new NoBrokersAvailableException

  def newBal(
    fs: Var[Traversable[SServiceFactory]],
    sr: StatsReceiver = NullStatsReceiver
  ): ServiceFactory[Unit, Int] = new StickyBalancer(
    Activity(fs.map(Activity.Ok(_))),
    statsReceiver = sr,
    emptyException = noBrokers,
    maxEffort = 1
  )
}

@RunWith(classOf[JUnitRunner])
class StickyBalancerTest extends FunSuite with StickySuite {
  case class LoadedFactory(id: Int) extends SServiceFactory {
    @volatile var stat: Status = Status.Open
    var load = 0
    var sum = 0
    var count = 0

    def apply(conn: ClientConnection) = {
      load += 1
      sum += load
      count += 1

      Future.value(new Service[Unit, Int] {
        def apply(req: Unit) = Future.value(id)
        override def close(deadline: Time) = {
          load -= 1
          sum += load
          count += 1
          Future.Done
        }
      })
    }

    def close(deadline: Time) = Future.Done
    override def toString = "LoadedFactory(%d, %d, %s)".format(id, load, stat)
    override def status = stat
  }

  test("Sticks to one backend") {
    val init = Vector.tabulate(N) { i => new LoadedFactory(i) }
    val bal = newBal(Var(init))
    for (_ <- 0 until R) bal()

    assert(init.filter(_.count == R).size == 1)
    assert(init.filter(_.count == 0).size == N - 1)
  }

  test("Empty load balancer throws") {
    val vec = Var(Vector.empty[LoadedFactory])
    val bal = newBal(vec)
    val exc = intercept[NoBrokersAvailableException] { Await.result(bal()) }
    assert(exc eq noBrokers)

    vec() :+= new LoadedFactory(0)
    for (_ <- 0 until R) Await.result(bal())
    assert(vec().head.load == R)

    vec() = Vector.empty
    intercept[NoBrokersAvailableException] { Await.result(bal()) }
  }

  test("Closing a node removes it from load balancing") {
    val init = Vector.tabulate(N) { i => new LoadedFactory(i) }
    val bal = newBal(Var.value(init))

    for (_ <- 0 until R) {
      assert(Await.result(bal()).status == Status.Open)
    }
    assert(init(0).count > 0)

    assert(init(0).status == Status.Open)
    init(0).stat = Status.Closed
    assert(init(0).status == Status.Closed)

    val init0count = init(0).count

    // There are no closed nodes returned from bal()
    for (_ <- 0 until R) {
      assert(Await.result(bal()).status == Status.Open)
    }

    // no new traffic has been sent to init(0) which is closed.
    assert(init0count == init(0).count)
  }

  test("Changing a node from Closed to Open re-adds it to load balancing") {
    val init = Vector.tabulate(2) { i => new LoadedFactory(i) }
    val bal = newBal(Var(init))

    // checkpoint for seeing how much traffic init(0) has seen
    var init0count = init(0).count

    assert(init(0).status == Status.Open)
    for (_ <- 0 until R) { bal() }
    assert(init(0).count > init0count)
    init0count = init(0).count

    init(0).stat = Status.Closed
    assert(init(0).status == Status.Closed)

    // There are no closed nodes
    for (_ <- 0 until R) {
      assert(Await.result(bal()).status == Status.Open)
    }

    for (_ <- 0 until R) { bal() }
    assert(init(0).count == init0count)

    // closing init(1) to swap back to re-opened node
    init(1).stat = Status.Closed

    init(0).stat = Status.Open
    init0count = init(0).count
    // And init(0) is now returned by the balancer
    for (_ <- 0 until R) { bal() }
    assert(init(0).count > init0count)
  }

  test("Closes") {
    val init = Vector.tabulate(N) { i => new LoadedFactory(i) }
    val bal = newBal(Var.value(init))
    // Give it some traffic.
    for (_ <- 0 until R) bal()
    Await.result(bal.close(), 5.seconds)
  }
}
