package com.twitter.finagle

import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util.{Future, Await}
import java.net.{UnknownHostException, InetAddress}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FixedInetResolverTest extends FunSuite {

  // The caching resolver (like the InetResolver, NilResolver)
  // should be installed by default and is capable of resolving addresses.
  test("Fixed Resolver is installed by default") {

    // It's safe to resolve a hard coded ip-address in CI as this will never result
    // in a DNS lookup.
    Resolver.eval(s"${FixedInetResolver.scheme}!1.2.3.4:100") match {
      case Name.Bound(va) =>
        val result = va.changes.filter(_ != Addr.Pending).toFuture()
        Await.result(result) match {
          case Addr.Bound(set, _) =>
            assert(set.contains(Address("1.2.3.4", 100)))
          case _ => fail("Should have been a bound address")
        }

      case _ => fail("Should have successfully resolved to a name")
    }
  }

  trait Ctx {
    var numLookups = 0
    var shouldFail = false
    val statsReceiver = new InMemoryStatsReceiver

    def resolve(host: String): Future[Seq[InetAddress]] = {
      numLookups += 1
      if (shouldFail) Future.exception(new UnknownHostException())
      else Future.value(Seq[InetAddress](InetAddress.getLoopbackAddress))
    }

    val resolver = new FixedInetResolver(FixedInetResolver.cache(resolve, Long.MaxValue), statsReceiver)
  }

  test("Caching resolver caches successes") {
    new Ctx {
      // make the same request n-times
      val hostnames = (1 to 5).map { i => s"1.2.3.$i:100" }
      val iterations = 10
      for(i <- 1 to iterations; hostname <- hostnames) {
        val request = resolver.bind(hostname).changes.filter(_ != Addr.Pending)

        Await.result(request.toFuture()) match {
          case Addr.Bound(_, _) =>
          case _ => fail("Resolution should have succeeded")
        }
      }

      // there should have only been 1 lookup, but all N successes
      assert(numLookups == 5)
      assert(statsReceiver.counter("successes")() == iterations * 5)
      assert(statsReceiver.gauges(Seq("cache", "size"))() == 5)
    }
  }


  test("Caching resolver respects cache size parameter") {
    new Ctx {
      val maxCacheSize = 1
      val cache = FixedInetResolver.cache(resolve, maxCacheSize)
      val resolver2 = new FixedInetResolver(cache, statsReceiver)
      // make the same request n-times

      def assertBound(hostname: String): Unit = {
        val request = resolver2.bind(hostname).changes.filter(_ != Addr.Pending)

        Await.result(request.toFuture()) match {
          case Addr.Bound(_, _) =>
          case _ => fail("Resolution should have succeeded")
        }
        cache.cleanUp()
      }

      val iterations = 10
      for(i <- 1 to iterations) {
        assertBound("1.2.3.4:100")
      }
      assert(numLookups == 1)
      assert(statsReceiver.counter("successes")() == iterations)
      assert(statsReceiver.gauges(Seq("cache", "size"))() == 1)
      assert(statsReceiver.gauges(Seq("cache", "evicts"))() == 0)

      // evict 1.2.3.4
      assertBound("1.2.3.5:100")
      assert(numLookups == 2)
      assert(statsReceiver.counter("successes")() == iterations + 1)
      assert(statsReceiver.gauges(Seq("cache", "size"))() == 1)
      assert(statsReceiver.gauges(Seq("cache", "evicts"))() == 1)

      // evicted
      assertBound("1.2.3.4:100")
      assert(numLookups == 3)
      assert(statsReceiver.counter("successes")() == iterations + 2)
      assert(statsReceiver.gauges(Seq("cache", "size"))() == 1)
      assert(statsReceiver.gauges(Seq("cache", "evicts"))() == 2)
    }
  }

  test("Caching resolver does not cache failures") {
    new Ctx {
      // make the same request n-times, but make them all fail
      val hostname = "1.2.3.4:100"
      shouldFail = true
      val iterations = 10
      for(i <- 1 to iterations) {
        val request = resolver.bind(hostname).changes.filter(_ != Addr.Pending)

        Await.result(request.toFuture()) match {
          case Addr.Neg =>
          case x => fail(s"Resolution should have failed: $x")
        }
      }

      // there should have only been N lookups, and N failures
      assert(numLookups == iterations)
      assert(statsReceiver.counter("failures")() == iterations)
    }
  }
}
