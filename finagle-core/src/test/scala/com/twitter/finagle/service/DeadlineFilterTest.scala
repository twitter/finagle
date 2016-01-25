package com.twitter.finagle.service

import com.twitter.finagle._
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util._
import com.twitter.util.TimeConversions._
import org.junit.runner.RunWith
import org.mockito.Matchers
import org.mockito.Mockito.when
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar


@RunWith(classOf[JUnitRunner])
class DeadlineFilterTest extends FunSuite with MockitoSugar {

  class DeadlineFilterHelper {
    val timer = new MockTimer
    val promise = new Promise[String]
    val service = new Service[String, String] {
      def apply(request: String) = promise
    }
    val statsReceiver = new InMemoryStatsReceiver
    val deadlineFilter = new DeadlineFilter[String, String](
      10.seconds, 10.seconds, 0.2, statsReceiver, Stopwatch.timeMillis)
    val deadlineService = deadlineFilter.andThen(service)
  }

  test("When there is no deadline set, DeadlineFilter should service the request") {
    val h = new DeadlineFilterHelper
    import h._

    promise.setValue("polo")

    val res = deadlineService("marco")
    assert(statsReceiver.counters.get(List("exceeded")) == None)
    assert(statsReceiver.counters.get(List("exceeded_beyond_tolerance")) == None)
    assert(statsReceiver.counters.get(List("rejected")) == None)
    assert(Await.result(res, 1.second) == "polo")

  }

  test("When the deadline is not exceeded, DeadlineFilter should service the request") {
    val h = new DeadlineFilterHelper
    import h._

    promise.setValue("polo")

    Time.withCurrentTimeFrozen { tc =>
      Contexts.broadcast.let(Deadline, Deadline.ofTimeout(1.seconds)) {
        tc.advance(500.milliseconds)
        for (i <- 0 until 5) Await.result(deadlineService("marco"), 1.second)
        val res = deadlineService("marco")
        assert(statsReceiver.counters.get(List("exceeded")) == None)
        assert(statsReceiver.counters.get(List("exceeded_beyond_tolerance")) == None)
        assert(statsReceiver.counters.get(List("rejected")) == None)
        assert(Await.result(res, 1.second) == "polo")
      }
    }
  }

  test("When the request has a deadline filter and is serviced, DeadlineFilter " +
    "should record budget remaining for the request") {
    val h = new DeadlineFilterHelper
    import h._

    promise.setValue("polo")

    Time.withCurrentTimeFrozen { tc =>
      Contexts.broadcast.let(Deadline, Deadline.ofTimeout(1.seconds)) {
        for (i <- 0 until 5) Await.result(deadlineService("marco"), 1.second)
        tc.advance(200.milliseconds)
        val res = deadlineService("marco")
        assert(statsReceiver.stats(Seq("deadline_budget_ms"))(5) == 800f)
        assert(Await.result(res, 1.second) == "polo")
      }
    }
  }

  test("When the request has a deadline filter and is serviced, DeadlineFilter " +
    "should record transit time for the request") {
    val h = new DeadlineFilterHelper
    import h._

    promise.setValue("polo")

    Time.withCurrentTimeFrozen { tc =>
      Contexts.broadcast.let(Deadline, Deadline.ofTimeout(1.seconds)) {
        for (i <- 0 until 5) Await.result(deadlineService("marco"), 1.second)
        tc.advance(200.milliseconds)
        val res = deadlineService("marco")
        assert(statsReceiver.stats(Seq("transit_latency_ms"))(5) == 200f)
        assert(Await.result(res, 1.second) == "polo")
      }
    }
  }

  test("When the request is rejected, DeadlineFilter should record " +
    "budget remaining for the request") {
    val h = new DeadlineFilterHelper
    import h._

    promise.setValue("polo")

    Time.withCurrentTimeFrozen { tc =>
      Contexts.broadcast.let(Deadline, Deadline.ofTimeout(1.seconds)) {
        for (i <- 0 until 5) Await.result(deadlineService("marco"), 1.second)
        tc.advance(2.seconds)
        assert(Await.result(deadlineService("marco"), 1.second) == "polo")
        assert(statsReceiver.stats(Seq("deadline_budget_ms")).length == 6)
      }
    }
  }

  test("When the request is rejected, DeadlineFilter should record " +
    "transit time for the request") {
    val h = new DeadlineFilterHelper
    import h._

    promise.setValue("polo")

    Time.withCurrentTimeFrozen { tc =>
      Contexts.broadcast.let(Deadline, Deadline.ofTimeout(1.seconds)) {
        for (i <- 0 until 5) Await.result(deadlineService("marco"), 1.second)
        tc.advance(2.seconds)
        assert(Await.result(deadlineService("marco"), 1.second) == "polo")
        assert(statsReceiver.stats(Seq("transit_latency_ms")).length == 6)
      }
    }
  }

  test("When the deadline is exceeded but beyond the tolerance threshold, " +
    "DeadlineFilter should service the request and increment the " +
    "exceeded_beyond_tolerance stat") {
    val h = new DeadlineFilterHelper
    import h._

    promise.setValue("polo")

    Time.withCurrentTimeFrozen { tc =>
      Contexts.broadcast.let(Deadline, Deadline.ofTimeout(1.seconds)) {
        tc.advance(100.seconds)
        val res = deadlineService("marco")
        assert(statsReceiver.counters.get(List("exceeded")) == None)
        assert(statsReceiver.counters.get(List("exceeded_beyond_tolerance")) == Some(1))
        assert(statsReceiver.counters.get(List("rejected")) == None)
        assert(Await.result(res, 1.second) == "polo")
      }
    }
  }

  test("When the deadline is exceeded and within the tolerance threshold, " +
    "but the reject token bucket contains too few tokens, DeadlineFilter " +
    "should service the request and increment the exceeded stat") {
      val h = new DeadlineFilterHelper
      import h._

      promise.setValue("polo")

      Time.withCurrentTimeFrozen { tc =>
        Contexts.broadcast.let(Deadline, Deadline.ofTimeout(1.seconds)) {
          for (i <- 0 until 3) Await.result(deadlineService("marco"), 1.second)
          tc.advance(2.seconds)
          val res = deadlineService("marco")
          assert(statsReceiver.counters.get(List("exceeded")) == Some(1))
          assert(statsReceiver.counters.get(List("exceeded_beyond_tolerance")) == None)
          assert(statsReceiver.counters.get(List("rejected")) == None)
          assert(Await.result(res, 1.second) == "polo")
        }
      }
    }

  test("When the deadline is exceeded and within the tolerance threshold, and " +
    "the reject token bucket contains sufficient tokens, DeadlineFilter " +
    "should service the request and increment the exceeded and rejected stats") {
      val h = new DeadlineFilterHelper
      import h._

      promise.setValue("polo")

      Time.withCurrentTimeFrozen { tc =>
        Contexts.broadcast.let(Deadline, Deadline.ofTimeout(1.seconds)) {
          for (i <- 0 until 5) Await.result(deadlineService("marco"), 1.second)
          tc.advance(2.seconds)
          assert(Await.result(deadlineService("marco"), 1.second) == "polo")
          assert(statsReceiver.counters.get(List("exceeded")) == Some(1))
          assert(statsReceiver.counters.get(List("exceeded_beyond_tolerance")) == None)
          assert(statsReceiver.counters.get(List("rejected")) == Some(1))
        }
      }
    }

  test("tokens added to reject bucket on request without deadline") {
    val h = new DeadlineFilterHelper
    import h._

    promise.setValue("polo")

    for (i <- 0 until 5) Await.result(deadlineService("marco"), 1.second)

    Time.withCurrentTimeFrozen { tc =>
      Contexts.broadcast.let(Deadline, Deadline.ofTimeout(1.seconds)) {
        tc.advance(2.seconds)

        // 5 tokens should have been added, so we should be able to reject
        assert(Await.result(deadlineService("marco"), 1.second) == "polo")
        assert(statsReceiver.counters.get(List("rejected")) == Some(1))
      }
    }
  }

  test("tokens added to reject bucket on request with expired deadline " +
    "greater than tolerance") {
    val h = new DeadlineFilterHelper
    import h._

    promise.setValue("polo")

    Time.withCurrentTimeFrozen { tc =>
      Contexts.broadcast.let(Deadline, Deadline.ofTimeout(1.seconds)) {
        tc.advance(30.seconds)
        for (i <- 0 until 5) Await.result(deadlineService("marco"), 1.second)
      }
      Contexts.broadcast.let(Deadline, Deadline.ofTimeout(1.seconds)) {
        tc.advance(2.seconds)

        // 5 tokens should have been added, so we should be able to reject
        assert(Await.result(deadlineService("marco"), 1.second) == "polo")
        assert(statsReceiver.counters.get(List("rejected")) == Some(1))
      }
    }
  }

  test("tokens are added to bucket on request with expired deadline greater " +
    "than tolerance when there are too few tokens to reject it") {
    val h = new DeadlineFilterHelper
    import h._

    promise.setValue("polo")

    Time.withCurrentTimeFrozen { tc =>
      Contexts.broadcast.let(Deadline, Deadline.ofTimeout(1.seconds)) {
        tc.advance(2.seconds)
        for (i <- 0 until 5) Await.result(deadlineService("marco"), 1.second)

        // 5 tokens should have been added, so we should be able to reject
        assert(Await.result(deadlineService("marco"), 1.second) == "polo")
        assert(statsReceiver.counters.get(List("rejected")) == Some(1))
      }
    }
  }

  test("tokens not added to bucket when request is rejected") {
    val h = new DeadlineFilterHelper
    import h._

    promise.setValue("polo")

    Time.withCurrentTimeFrozen { tc =>
      Contexts.broadcast.let(Deadline, Deadline.ofTimeout(1.seconds)) {
        for (i <- 0 until 5) Await.result(deadlineService("marco"), 1.second)
        tc.advance(2.seconds)
        assert(Await.result(deadlineService("marco"), 1.second) == "polo")
        assert(statsReceiver.counters.get(List("rejected")) == Some(1))
      }

      // If we add 4 more tokens, should still not be able to reject again.
      Contexts.broadcast.let(Deadline, Deadline.ofTimeout(1.seconds)) {
        for (i <- 0 until 4) Await.result(deadlineService("marco"), 1.second)
        tc.advance(2.seconds)
        Await.result(deadlineService("marco"), 1.second)
        assert(statsReceiver.counters.get(List("rejected")) == Some(1))
      }
    }
  }

  test("tokens added to reject bucket expire") {
    val h = new DeadlineFilterHelper
    import h._

    promise.setValue("polo")

    Time.withCurrentTimeFrozen { tc =>
      for (i <- 0 until 5) Await.result(deadlineService("marco"), 1.second)
      tc.advance(11.seconds)

      // tokens have expired so we should not be able to reject
      Contexts.broadcast.let(Deadline, Deadline.ofTimeout(1.seconds)) {
        tc.advance(2.seconds)
        Await.result(deadlineService("marco"), 1.second)
        assert(statsReceiver.counters.get(List("rejected")) == None)
      }
    }
  }

  test("param") {
    import DeadlineFilter._

    val p: Param = Param(1.second, 0.5)

    val ps: Stack.Params = Stack.Params.empty + p
    assert(ps.contains[Param])
    assert((ps[Param] match { case Param(t, d) => (t, d)}) == ((1.second, 0.5)))
  }

  test("module configured correctly using stack params") {
    val h = new DeadlineFilterHelper
    import h._

    val underlyingService = mock[Service[String, String]]
    when(underlyingService(Matchers.anyString)) thenReturn Future.value("polo")

    val underlying = mock[ServiceFactory[String, String]]
    when(underlying()) thenReturn Future.value(underlyingService)

    val s: Stack[ServiceFactory[String, String]] =
      DeadlineFilter.module[String, String].toStack(Stack.Leaf(Stack.Role("Service"), underlying))

    val ps: Stack.Params = Stack.Params.empty + param.Stats(h.statsReceiver)

    val service = s.make(ps + DeadlineFilter.Param(10.seconds, 0.5)).toService

    Time.withCurrentTimeFrozen { tc =>
      Contexts.broadcast.let(Deadline, Deadline.ofTimeout(5.seconds)) {
        Await.result(service("marco"), 1.second)
        Await.result(service("marco"), 1.second)
        tc.advance(7.seconds)
        assert(Await.result(service("marco"), 1.second) == "polo")
        assert(statsReceiver.counters.get(List("admission_control", "deadline", "exceeded")) == Some(1))
        assert(statsReceiver.counters.get(List("admission_control", "deadline", "exceeded_beyond_tolerance")) == None)
        assert(statsReceiver.counters.get(List("admission_control", "deadline", "rejected")) == Some(1))
      }
    }
  }
}
