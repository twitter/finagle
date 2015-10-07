package com.twitter.finagle.server

import com.twitter.conversions.time._
import com.twitter.finagle.{Service, ServiceFactory, Deadline, Stack}
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.param.Stats
import com.twitter.finagle.server.StackServer
import com.twitter.finagle.service.TimeoutFilter
import com.twitter.finagle.stack.Endpoint
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util.{Await, Future, Time}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class StackServerTest extends FunSuite {
  test("Deadline isn't changed until after it's recorded") {
    val echo = ServiceFactory.const(Service.mk[Unit, Deadline] { unit =>
      Future.value(Contexts.broadcast(Deadline))
    })
    val stack = StackServer.newStack[Unit, Deadline] ++ Stack.Leaf(Endpoint, echo)
    val statsReceiver = new InMemoryStatsReceiver
    val factory = stack.make(StackServer.defaultParams + TimeoutFilter.Param(1.second) + Stats(statsReceiver))
    val svc = Await.result(factory(), 5.seconds)
    Time.withCurrentTimeFrozen { ctl =>
      Contexts.broadcast.let(Deadline, Deadline.ofTimeout(5.seconds)) {
        ctl.advance(1.second)
        val result = svc(())

        // we should be one second ahead
        assert(statsReceiver.stats(Seq("transit_latency_ms"))(0) == 1.second.inMilliseconds.toFloat)

        // but the deadline inside the service's closure should be updated
        assert(Await.result(result) == Deadline.ofTimeout(1.second))
      }
    }
  }
}
