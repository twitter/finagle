package com.twitter.finagle

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.stats.DefaultStatsReceiver
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util._
import java.net.InetAddress
import java.net.UnknownHostException
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import org.scalatest.funsuite.AnyFunSuite

class InetResolverTest extends AnyFunSuite {
  val statsReceiver = new InMemoryStatsReceiver

  val dnsResolver = new DnsResolver(statsReceiver, FuturePool.unboundedPool)
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
    val inetResolverWithPool =
      new InetResolver(resolveLoopback, DefaultStatsReceiver, Some(pollInterval))

    val maxWaitTimeout = 10.seconds
    val addr = inetResolverWithPool.bind("127.0.0.1:80")
    val f = addr.changes.filter(_ != Addr.Pending).toFuture
    Await.result(f, 10.seconds) match {
      case Addr.Bound(b, meta) if meta.isEmpty =>
        assert(b.contains(Address("127.0.0.1", 80)))
      case _ => fail()
    }

    // Should be completed immediately
    assert(latch.await(maxWaitTimeout.inMilliseconds, TimeUnit.MILLISECONDS))
  }

  test("Infinite resolutions can be aborted without a poll interval") {
    val resolving = Promise[Unit]()
    val neverResolvesAddresses = Promise[Seq[InetAddress]]()
    neverResolvesAddresses.setInterruptHandler {
      case t: Throwable => neverResolvesAddresses.updateIfEmpty(Throw(t))
    }

    val resolveHost = { _: String =>
      resolving.setDone()
      neverResolvesAddresses
    }

    val resolver = new InetResolver(resolveHost, DefaultStatsReceiver, None)

    val addr = resolver.bind("localhost:1234")
    val c = addr.changes.respond { _ => () }

    Await.result(resolving, 5.seconds)
    Await.result(c.close(), 10.seconds)

    intercept[InterruptedException] {
      Await.result(neverResolvesAddresses, 5.seconds)
    }
  }

  test("Infinite resolutions can be aborted with a poll interval") {
    val resolving = Promise[Unit]()
    val neverResolvesAddresses = Promise[Seq[InetAddress]]()
    neverResolvesAddresses.setInterruptHandler {
      case t: Throwable => neverResolvesAddresses.updateIfEmpty(Throw(t))
    }

    val resolveHost = { _: String =>
      resolving.setDone()
      neverResolvesAddresses
    }

    val resolver = new InetResolver(resolveHost, DefaultStatsReceiver, Some(2.seconds))

    val addr = resolver.bind("localhost:1234")
    val c = addr.changes.respond { _ => () }

    Await.result(resolving, 5.seconds)
    Await.result(c.close(), 10.seconds)

    intercept[InterruptedException] {
      Await.result(neverResolvesAddresses, 5.seconds)
    }
  }
}
