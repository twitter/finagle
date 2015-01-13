package com.twitter.finagle

import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util.Await
import java.net.InetSocketAddress
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class InetResolverTest extends FunSuite {
  val statsReceiver = new InMemoryStatsReceiver
  val resolver = InetResolver(statsReceiver)

  test("host not found") {
    val addr = resolver.bind("no_TLDs_for_old_humans:80")
    val f = addr.changes.filter(_ == Addr.Neg).toFuture
    assert(Await.result(f) === Addr.Neg)
    assert(statsReceiver.counter("inet", "dns", "failures")() > 0)
  }

  test("resolution failure") {
    val addr = resolver.bind("no_port_number")
    val f = addr.changes.filter(_ != Addr.Pending).toFuture
    Await.result(f) match {
      case Addr.Failed(_) =>
      case _ => fail()
    }
  }

  test("successful resolution") {
    val addr = resolver.bind("localhost:80") // name resolution only, not bound
    val f = addr.changes.filter(_ != Addr.Pending).toFuture
    Await.result(f) match {
      case Addr.Bound(b, meta) if meta.isEmpty =>
        assert(b === Set(WeightedSocketAddress(new InetSocketAddress("localhost", 80), 1L)))
      case _ => fail()
    }
    assert(statsReceiver.counter("inet", "dns", "successes")() > 0)
    assert(statsReceiver.stat("inet", "dns", "lookup_ms")().size > 0)
  }
}
