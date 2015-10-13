package com.twitter.finagle

import com.twitter.conversions.time._
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util.{Future, Await}
import java.net.{UnknownHostException, InetAddress, InetSocketAddress}
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
            assert(set.contains(WeightedSocketAddress(new InetSocketAddress("1.2.3.4", 100), 1L)))
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

    val resolver = new FixedInetResolver(statsReceiver, Some(resolve))
  }

  test("Caching resolver caches successes") {
    new Ctx {
      // make the same request n-times
      val hostname = "1.2.3.4:100"
      val iterations = 10
      for(i <- 1 to iterations) {
        val request = resolver.bind(hostname).changes.filter(_ != Addr.Pending)

        Await.result(request.toFuture(), 2.milliseconds) match {
          case Addr.Bound(_, _) =>
          case _ => fail("Resolution should have succeeded")
        }
      }

      // there should have only been 1 lookup, but all N successes
      assert(numLookups == 1)
      assert(statsReceiver.counter("inet", "dns", "successes")() == iterations)
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

        Await.result(request.toFuture(), 2.milliseconds) match {
          case Addr.Neg =>
          case x => fail(s"Resolution should have failed: $x")
        }
      }

      // there should have only been N lookups, and N failures
      assert(numLookups == iterations)
      assert(statsReceiver.counter("inet", "dns", "failures")() == iterations)
    }
  }
}
