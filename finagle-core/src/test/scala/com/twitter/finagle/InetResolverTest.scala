package com.twitter.finagle

import com.twitter.conversions.time._
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util.{Future, Await}
import java.net.{UnknownHostException, InetAddress, InetSocketAddress}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class InetResolverTest extends FunSuite {
  val statsReceiver = new InMemoryStatsReceiver

  val resolver = new InetResolver(statsReceiver, None) {
    override def resolveHost(host: String): Future[Seq[InetAddress]] = {
      if (host.equals("localhost")) super.resolveHost(host)
      else Future.exception(new UnknownHostException())
    }
  }

  test("host not found") {
    val addr = resolver.bind("no_TLDs_for_old_humans:80")
    val f = addr.changes.filter(_ == Addr.Neg).toFuture
    assert(Await.result(f) == Addr.Neg)
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

  test("partial resolution success") {
    val addr = resolver.bind("bad_host_name:100, localhost:80")
    val f = addr.changes.filter(_ != Addr.Pending).toFuture
    Await.result(f, 10.seconds) match {
      case Addr.Bound(b, meta) if meta.isEmpty =>
        assert(b.contains(WeightedSocketAddress(new InetSocketAddress("localhost", 80), 1L)))
      case _ => fail()
    }
    assert(statsReceiver.counter("inet", "dns", "successes")() > 0)
    assert(statsReceiver.stat("inet", "dns", "lookup_ms")().size > 0)
  }

  test("empty host list returns an empty set") {
    val addr = resolver.bind("")
    val f = addr.changes.filter(_ != Addr.Pending).toFuture
    Await.result(f) match {
      case Addr.Bound(b, meta) if b.isEmpty =>
      case _ => fail()
    }
  }

  test("successful resolution") {
    val addr = resolver.bind("localhost:80") // name resolution only, not bound
    val f = addr.changes.filter(_ != Addr.Pending).toFuture
    Await.result(f) match {
      case Addr.Bound(b, meta) if meta.isEmpty =>
        assert(b.contains(WeightedSocketAddress(new InetSocketAddress("localhost", 80), 1L)))
      case _ => fail()
    }
    assert(statsReceiver.counter("inet", "dns", "successes")() > 0)
    assert(statsReceiver.stat("inet", "dns", "lookup_ms")().size > 0)
  }

}
