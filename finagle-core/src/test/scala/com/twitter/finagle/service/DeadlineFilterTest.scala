package com.twitter.finagle.service

import com.twitter.finagle._
import com.twitter.finagle.context.{Deadline, Contexts}
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util._
import com.twitter.util.TimeConversions._
import org.junit.runner.RunWith
import org.scalatest.{OneInstancePerTest, FunSuite}
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class DeadlineFilterTest extends FunSuite with MockitoSugar with OneInstancePerTest {
  import DeadlineFilter.DeadlineExceededException

  val promise = new Promise[String]
  val service = new Service[String, String] {
    def apply(request: String) = promise
  }
  val statsReceiver = new InMemoryStatsReceiver
  val deadlineFilter = new DeadlineFilter[String, String](
    rejectPeriod = 10.seconds,
    maxRejectFraction = 0.2,
    statsReceiver = statsReceiver,
    nowMillis = Stopwatch.timeMillis
  )

  val deadlineService = deadlineFilter.andThen(service)
  promise.setValue("polo")

  test("DeadlineFilter should service the request when no deadline is set") {
    val res = deadlineService("marco")
    assert(statsReceiver.counters.get(List("exceeded")) == None)
    assert(Await.result(res, 1.second) == "polo")
  }

  test("DeadlineFilter should record expired deadline for the request") {
    Time.withCurrentTimeFrozen { tc =>
      Contexts.broadcast.let(Deadline, Deadline.ofTimeout(200.milliseconds)) {
        tc.advance(1.second)
        val res = deadlineService("marco")
        assert(statsReceiver.stats(Seq("expired_ms"))(0) == 800f)
        assert(Await.result(res, 1.second) == "polo")
      }
    }
  }

  test("DeadlineFilter should ignore non-expired deadline for the request") {
    Time.withCurrentTimeFrozen { tc =>
      Contexts.broadcast.let(Deadline, Deadline.ofTimeout(1.second)) {
        tc.advance(200.milliseconds)
        val res = deadlineService("marco")
        assert(!statsReceiver.stats.contains(Seq("expired_ms")))
        assert(Await.result(res, 1.second) == "polo")
      }
    }
  }

  test(
    "When the deadline is exceeded but the reject token bucket contains too few tokens, " +
      "DeadlineFilter should service the request and increment the exceeded stat"
  ) {
    Time.withCurrentTimeFrozen { tc =>
      Contexts.broadcast.let(Deadline, Deadline.ofTimeout(1.seconds)) {
        for (i <- 0 until 3) Await.result(deadlineService("marco"), 1.second)
        tc.advance(2.seconds)
        assert(Await.result(deadlineService("marco"), 1.second) == "polo")
        assert(statsReceiver.counters.get(List("exceeded")) == Some(1))
        assert(statsReceiver.counters.get(List("exceeded_beyond_tolerance")) == None)
        assert(statsReceiver.counters.get(List("rejected")) == None)
      }
    }
  }

  test(
    "When the deadline is exceeded and the reject token bucket contains sufficient tokens," +
      "DeadlineFilter should not service the request and increment the exceeded and rejected stats"
  ) {
    Time.withCurrentTimeFrozen { tc =>
      Contexts.broadcast.let(Deadline, Deadline.ofTimeout(1.seconds)) {
        for (i <- 0 until 5) Await.result(deadlineService("marco"), 1.second)
        tc.advance(2.seconds)
        val f = intercept[DeadlineExceededException] {
          Await.result(deadlineService("marco"), 1.second)
        }
        assert(f.getMessage.contains("exceeded request deadline"))
        assert(statsReceiver.counters.get(List("exceeded")) == Some(1))
        assert(statsReceiver.counters.get(List("exceeded_beyond_tolerance")) == None)
        assert(statsReceiver.counters.get(List("rejected")) == Some(1))
      }
    }
  }

  test("tokens added to reject bucket on request without deadline") {
    for (i <- 0 until 5) Await.result(deadlineService("marco"), 1.second)

    Time.withCurrentTimeFrozen { tc =>
      Contexts.broadcast.let(Deadline, Deadline.ofTimeout(1.seconds)) {
        tc.advance(2.seconds)

        // 5 tokens should have been added, so we should be able to reject
        val f = intercept[DeadlineExceededException] {
          Await.result(deadlineService("marco"), 1.second)
        }
        assert(f.isFlagged(Failure.NonRetryable))
        assert(f.isFlagged(Failure.Rejected))
        assert(statsReceiver.counters.get(List("rejected")) == Some(1))
      }
    }
  }

  test(
    "tokens are added to bucket on request with expired deadline " +
      "when there are too few tokens to reject it"
  ) {
    Time.withCurrentTimeFrozen { tc =>
      Contexts.broadcast.let(Deadline, Deadline.ofTimeout(1.seconds)) {
        tc.advance(2.seconds)
        for (i <- 0 until 5) Await.result(deadlineService("marco"), 1.second)

        // 5 tokens should have been added, so we should be able to reject
        val f = intercept[DeadlineExceededException] {
          Await.result(deadlineService("marco"), 1.second)
        }
        assert(f.isFlagged(Failure.NonRetryable))
        assert(Await.result(deadlineService("marco"), 1.second) == "polo")
        assert(statsReceiver.counters.get(List("rejected")) == Some(1))
      }
    }
  }

  test("tokens not added to bucket when request is rejected") {
    Time.withCurrentTimeFrozen { tc =>
      Contexts.broadcast.let(Deadline, Deadline.ofTimeout(1.seconds)) {
        for (i <- 0 until 5) Await.result(deadlineService("marco"), 1.second)
        tc.advance(2.seconds)
        val f = intercept[DeadlineExceededException] {
          Await.result(deadlineService("marco"), 1.second)
        }
        assert(f.isFlagged(Failure.NonRetryable))
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

    val p: Param = Param(0.5)

    val ps: Stack.Params = Stack.Params.empty + p
    assert(ps.contains[Param])
    assert((ps[Param] match { case Param(d) => (d) }) == 0.5)
  }
}
