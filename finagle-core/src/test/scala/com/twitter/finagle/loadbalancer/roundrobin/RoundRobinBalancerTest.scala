package com.twitter.finagle.loadbalancer.roundrobin

import com.twitter.conversions.DurationOps._
import com.twitter.finagle._
import com.twitter.util.{Function => _, _}
import org.scalatest.funsuite.AnyFunSuite

class RoundRobinBalancerTest extends AnyFunSuite with RoundRobinSuite {
  case class LoadedFactory(id: Int) extends RRServiceFactory {
    @volatile var stat: Status = Status.Open
    var load = 0
    var sum = 0
    var count = 0

    def meanLoad: Double = if (count == 0) 0 else sum.toDouble / count.toDouble

    def apply(conn: ClientConnection): Future[Service[Unit, Int]] = {
      load += 1
      sum += load
      count += 1

      Future.value(new Service[Unit, Int] {
        def apply(req: Unit): Future[Int] = Future.value(id)
        override def close(deadline: Time): Future[Unit] = {
          load -= 1
          sum += load
          count += 1
          Future.Done
        }
      })
    }

    def close(deadline: Time): Future[Unit] = Future.Done
    override def toString: String = "LoadedFactory(%d, %d, %s)".format(id, load, stat)
    override def status: Status = stat
  }

  test("Balances evenly") {
    val init = Vector.tabulate(N) { i => LoadedFactory(i) }
    val bal = newBal(Var(init))
    for (_ <- 0 until R) bal()
    assertEven(init)
  }

  test("Empty load balancer throws") {
    val vec = Var(Vector.empty[LoadedFactory])
    val bal = newBal(vec)
    val exc = intercept[NoBrokersAvailableException] { Await.result(bal()) }
    assert(exc eq noBrokers)

    vec() :+= LoadedFactory(0)
    for (_ <- 0 until R) Await.result(bal())
    assert(vec().head.load == R)

    vec() = Vector.empty
    intercept[NoBrokersAvailableException] { Await.result(bal()) }
  }

  test("Closing a node removes it from load balancing") {
    val init = Vector.tabulate(N) { i => LoadedFactory(i) }
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
    val init = Vector.tabulate(N) { i => LoadedFactory(i) }
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

    init(0).stat = Status.Open
    init0count = init(0).count
    // And init(0) is now returned by the balancer
    for (_ <- 0 until R) { bal() }
    assert(init(0).count > init0count)
  }

  test("Closes") {
    val init = Vector.tabulate(N) { i => LoadedFactory(i) }
    val bal = newBal(Var.value(init))
    // Give it some traffic.
    for (_ <- 0 until R) bal()
    Await.result(bal.close(), 5.seconds)
  }
}
