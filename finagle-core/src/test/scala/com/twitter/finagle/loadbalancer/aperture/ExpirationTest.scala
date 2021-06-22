package com.twitter.finagle.loadbalancer.aperture

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Address
import com.twitter.finagle.loadbalancer.{EndpointFactory, LazyEndpointFactory}
import com.twitter.finagle.ServiceFactoryProxy
import com.twitter.finagle.stats.{InMemoryStatsReceiver, StatsReceiver}
import com.twitter.finagle.util.Rng
import com.twitter.util._
import org.scalatest.funsuite.FixtureAnyFunSuite

class ExpirationTest extends FixtureAnyFunSuite with ApertureSuite {

  /**
   * An aperture load balancer which mixes in expiration but no
   * controller or load metric. We manually have to adjust the
   * aperture to test for nodes falling in and out of the window.
   */
  private class ExpiryBal(
    val idleTime: Duration = 1.minute,
    val mockTimer: MockTimer = new MockTimer,
    val stats: InMemoryStatsReceiver = new InMemoryStatsReceiver)
      extends TestBal
      with Expiration[Unit, Unit] {

    val manageEndpoints: Boolean = false
    def expired: Long = stats.counters(Seq("expired"))
    def noExpired: Boolean = stats.counters(Seq("expired")) == 0

    protected def endpointIdleTime: Duration = idleTime / 2
    protected def statsReceiver: StatsReceiver = stats

    private[this] val expiryTask = newExpiryTask(mockTimer)

    case class Node(factory: EndpointFactory[Unit, Unit])
        extends ServiceFactoryProxy[Unit, Unit](factory)
        with ExpiringNode
        with ApertureNode[Unit, Unit] {
      override def tokenRng: Rng = rng
      def load: Double = 0
      def pending: Int = 0
      override val token: Int = 0
    }

    protected def newNode(factory: EndpointFactory[Unit, Unit]): Node = Node(factory)

    override def close(when: Time) = {
      expiryTask.cancel()
      super.close(when)
    }
  }

  private def newLazyEndpointFactory(sf: Factory) =
    new LazyEndpointFactory(() => sf, Address.Failed(new Exception))

  case class FixtureParam(tc: TimeControl)
  def withFixture(test: OneArgTest) =
    Time.withCurrentTimeFrozen { tc => test(FixtureParam(tc)) }

  test("does not expire uninitialized nodes") { f =>
    val bal = new ExpiryBal
    val ep0, ep1 = Factory(0)
    bal.update(Vector(ep0, ep1))
    assert(bal.aperturex == 1)

    f.tc.advance(bal.idleTime)
    bal.mockTimer.tick()
    assert(bal.noExpired)
  }

  test("expired counter is incremented once per close") { f =>
    val bal = new ExpiryBal
    val eps = Vector(Factory(0), Factory(1))
    bal.update(eps.map(newLazyEndpointFactory))
    bal.adjustx(1)
    assert(bal.aperturex == 2)

    (0 to 10).foreach { _ => Await.result(bal(), 5.seconds).close() }
    bal.adjustx(-1)
    assert(bal.aperturex == 1)

    f.tc.advance(bal.idleTime)
    bal.mockTimer.tick()
    assert(bal.expired == 1)
    assert(eps.map(_.numCloses).sum == 1)

    // Although calling `remake` on an already expired node is harmless,
    // it makes the expired counter hard to reason about, so we want to
    // ensure that we only increment it once per expiration.
    (0 to 100).foreach { _ =>
      f.tc.advance(bal.idleTime)
      bal.mockTimer.tick()
      assert(bal.expired == 1)
      assert(eps.map(_.numCloses).sum == 1)
    }
  }

  test("expires nodes outside of aperture") { f =>
    val bal = new ExpiryBal

    val eps = Vector.tabulate(10) { i => Factory(i) }
    bal.update(eps.map(newLazyEndpointFactory))
    bal.adjustx(eps.size)
    assert(bal.aperturex == eps.size)

    // we rely on p2c to ensure that each endpoint gets
    // a request for service acquisition.
    def checkoutLoop(): Unit = (0 to 100).foreach { _ => Await.result(bal(), 5.seconds).close() }

    checkoutLoop()
    assert(eps.filter(_.total > 0).size == eps.size)

    // since our aperture covers all nodes no endpoint should go idle
    f.tc.advance(bal.idleTime)
    bal.mockTimer.tick()
    assert(bal.noExpired)

    eps.foreach(_.clear())
    // set idle time on each node.
    checkoutLoop()
    // shrink aperture so some nodes qualify for expiration.
    bal.adjustx(-eps.size / 2)
    // tick the timer partially and no expirations
    f.tc.advance(bal.idleTime / 4)
    bal.mockTimer.tick()
    assert(bal.noExpired)
    // tick time fully
    f.tc.advance(bal.idleTime)
    bal.mockTimer.tick()
    assert(bal.expired == eps.size / 2)
    assert(eps.map(_.numCloses).sum == eps.size / 2)
  }

  test("idle time measured only on last response") { f =>
    val bal = new ExpiryBal
    val eps = Vector(Factory(0), Factory(1))
    bal.update(eps)
    bal.adjustx(1)
    assert(bal.aperturex == 2)

    val svcs = for (_ <- 0 until 100) yield { Await.result(bal(), 5.seconds) }
    bal.adjustx(-1)
    assert(bal.aperturex == 1)
    assert(eps.map(_.outstanding).sum == 100)

    val svcs0 = svcs.collect { case svc if svc.toString == "Service(0)" => svc }
    val svcs1 = svcs.collect { case svc if svc.toString == "Service(1)" => svc }

    for (svc <- svcs0 ++ svcs1.init) {
      Await.result(svc.close(), 5.seconds)
      f.tc.advance(bal.idleTime)
      bal.mockTimer.tick()
      assert(bal.noExpired)
    }

    assert(eps.map(_.outstanding).sum == 1)
    Await.result(svcs1.last.close(), 5.seconds)
    assert(eps.map(_.outstanding).sum == 0)

    f.tc.advance(bal.idleTime)
    bal.mockTimer.tick()
    assert(bal.expired == 1)
  }
}
