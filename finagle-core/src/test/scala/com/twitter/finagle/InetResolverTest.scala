package com.twitter.finagle

import com.twitter.conversions.time._
import com.twitter.finagle.stats.{DefaultStatsReceiver, InMemoryStatsReceiver}
import com.twitter.util._
import java.net.{InetAddress, UnknownHostException}

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class InetResolverTest extends FunSuite {
  val statsReceiver = new InMemoryStatsReceiver

  val dnsResolver = new DnsResolver(statsReceiver, FuturePool.unboundedPool)
  def resolveHost(host: String): Future[Seq[InetAddress]] = {
    if (host.isEmpty || host.equals("localhost")) dnsResolver(host)
    else Future.exception(new UnknownHostException())
  }
  val resolver = new InetResolver(resolveHost, statsReceiver, None, FuturePool.unboundedPool)

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

  test("updates using custom future pool") {
    class TestPool(latch: CountDownLatch) extends FuturePool {
      private val delegate = FuturePool.unboundedPool
      override def apply[T](f: => T): Future[T] = {
        latch.countDown()
        delegate(f)
      }
    }

    val latch = new CountDownLatch(1) // DnsResolver will use the pool
    val resolvePool = new TestPool(latch)

    val dnsResolverWithPool = new DnsResolver(DefaultStatsReceiver, resolvePool)

    def resolveLoopback(host: String): Future[Seq[InetAddress]] = {
      if (host.equals("127.0.0.1")) dnsResolverWithPool(host)
      else Future.exception(new UnknownHostException())
    }

    val pollInterval = 100.millis
    val inetResolverWithPool = new InetResolver(
      resolveLoopback, DefaultStatsReceiver, Some(pollInterval), resolvePool)

    val maxWaitTimeout = 10.seconds
    val addr = inetResolverWithPool.bind("127.0.0.1:80")
    val f = addr.changes.filter(_ != Addr.Pending).toFuture
    Await.result(f, 10.seconds) match {
      case Addr.Bound(b, meta) if meta.isEmpty =>
        assert(b.contains(Address("127.0.0.1", 80)))
      case _ => fail()
    }

    // Should be completed immediately
    assert(latch.await(maxWaitTimeout))
  }
}
