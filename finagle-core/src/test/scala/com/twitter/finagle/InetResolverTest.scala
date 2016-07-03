package com.twitter.finagle

import com.twitter.conversions.time._
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util.{Future, Await}
import java.net.{UnknownHostException, InetAddress}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class InetResolverTest extends FunSuite {
  val statsReceiver = new InMemoryStatsReceiver

  val dnsResolver = new DnsResolver(statsReceiver)
  def resolveHost(host: String): Future[Seq[InetAddress]] = {
    if (host.isEmpty || host.equals("localhost")) dnsResolver(host)
    else Future.exception(new UnknownHostException())
  }
  val resolver = new InetResolver(resolveHost, statsReceiver, None)

  test("local address") {
    val empty = resolver.bind(":9990")
    assert(empty.sample() == Addr.Bound(Address(9990)))

    val localhost = resolver.bind("localhost:9990")
    assert(localhost.sample() == Addr.Bound(Address(9990)))
  }

  test("host not found") {
    val addr = resolver.bind("no_TLDs_for_old_humans:80")
    val f = addr.changes.filter(_ == Addr.Neg).toFuture
    assert(Await.result(f) == Addr.Neg)
    assert(statsReceiver.counter("failures")() > 0)
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
        assert(b.contains(Address("localhost", 80)))
      case _ => fail()
    }
    assert(statsReceiver.counter("successes")() > 0)
    assert(statsReceiver.stat("lookup_ms")().size > 0)
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
        assert(b.contains(Address("localhost", 80)))
      case _ => fail()
    }
    assert(statsReceiver.counter("successes")() > 0)
    assert(statsReceiver.stat("lookup_ms")().size > 0)
  }

}
