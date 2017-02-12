package com.twitter.finagle.client

import com.twitter.finagle.{Addr, Service, Stack, StackBuilder, ServiceFactory}
import com.twitter.finagle.client.AddrMetadataExtraction.AddrMetadata
import com.twitter.finagle.param.Stats
import com.twitter.finagle.stack.nilStack
import com.twitter.finagle.stats.{InMemoryStatsReceiver, StatsReceiver}
import com.twitter.util.{Await, Future}
import org.junit.runner.RunWith
import org.scalatest.junit.{AssertionsForJUnit, JUnitRunner}
import org.scalatest.FunSuite

@RunWith(classOf[JUnitRunner])
class StatsScopingTest extends FunSuite with AssertionsForJUnit {
  class Ctx {
    val stats = new InMemoryStatsReceiver

    def mkCounterService(stats: StatsReceiver) =
      Service.mk[String, Unit] { key =>
        stats.counter(key).incr()
        Future.Done
      }

    val counterServiceModule = new Stack.Module[ServiceFactory[String, Unit]] {
      val role = Stack.Role("counterServiceModule")
      val description = "Produce a test service that increments stats counters"
      val parameters = Seq(implicitly[Stack.Param[StatsScoping.Scoper]])
      def make(params: Stack.Params, next: Stack[ServiceFactory[String, Unit]]) = {
        val Stats(stats0) = params[Stats]
        Stack.Leaf(this, ServiceFactory.const(mkCounterService(stats0)))
      }
    }

    def mkService(metadata: Addr.Metadata)(scoper: StatsScoping.ScoperFunction) = {
      val factory = new StackBuilder[ServiceFactory[String, Unit]](nilStack[String, Unit])
        .push(counterServiceModule)
        .push(StatsScoping.module)
        .make(Stack.Params.empty
          + Stats(stats)
          + StatsScoping.Scoper(scoper)
          + AddrMetadata(metadata))

      Await.result(factory())
    }
  }

  test("scope based on metadata")(new Ctx {
    val service = mkService(Addr.Metadata("zone" -> "foo")) { (stats0, metadata) =>
      stats0.scope(metadata("zone").toString)
    }
    assert(Map.empty == stats.counters)

    Await.result(service("bar"))
    Await.result(service("baz"))

    assert(Map(Seq("foo", "bar") -> 1, Seq("foo", "baz") -> 1) == stats.counters)
  })
}
