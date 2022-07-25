package com.twitter.finagle.service

import com.twitter.finagle._
import com.twitter.finagle.context.Deadline
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util._
import com.twitter.conversions.DurationOps._
import org.scalatest.OneInstancePerTest
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

class DeadlineFilterTest extends AnyFunSuite with MockitoSugar with OneInstancePerTest {
  import DeadlineFilter.DeadlineExceededException

  val promise = new Promise[String]
  promise.setValue("polo")

  val service = new Service[String, String] {
    def apply(request: String) = promise
  }
  val statsReceiver = new InMemoryStatsReceiver
  def mkDeadlineFilter(): DeadlineFilter[String, String] = new DeadlineFilter[String, String](
    rejectPeriod = 10.seconds,
    maxRejectFraction = 0.2,
    statsReceiver = statsReceiver,
    nowMillis = Stopwatch.timeMillis,
    isDarkMode = false
  )
  def mkDeadlineService(): Service[String, String] = mkDeadlineFilter().andThen(service)

  def await[A](f: Future[A]): A = Await.result(f, 1.second)

  test("DeadlineFilter should service the request when no deadline is set") {
    val deadlineService = mkDeadlineService()
    val res = deadlineService("marco")
    assert(statsReceiver.counters(List("exceeded")) == 0)
    assert(await(res) == "polo")
  }

  test("DeadlineFilter should record expired deadline for the request") {
    Time.withCurrentTimeFrozen { tc =>
      val deadlineService = mkDeadlineService()
      Contexts.broadcast.let(Deadline, Deadline.ofTimeout(200.milliseconds)) {
        tc.advance(1.second)
        val res = deadlineService("marco")
        assert(statsReceiver.stats(Seq("expired_ms"))(0) == 800f)
        assert(statsReceiver.stats(Seq("remaining_ms")) == List.empty)
        assert(await(res) == "polo")
      }
    }
  }

  test("DeadlineFilter should record remaining time on non-expired deadlines") {
    Time.withCurrentTimeFrozen { tc =>
      val deadlineService = mkDeadlineService()
      Contexts.broadcast.let(Deadline, Deadline.ofTimeout(1200.milliseconds)) {
        tc.advance(1.second)
        val res = deadlineService("marco")
        assert(statsReceiver.stats(Seq("remaining_ms"))(0) == 200f)
        assert(statsReceiver.stats(Seq("expired_ms")) == List.empty)
        assert(await(res) == "polo")
      }
    }
  }

  test("DeadlineFilter should ignore non-expired deadline for the request") {
    Time.withCurrentTimeFrozen { tc =>
      val deadlineService = mkDeadlineService()
      Contexts.broadcast.let(Deadline, Deadline.ofTimeout(1.second)) {
        tc.advance(200.milliseconds)
        val res = deadlineService("marco")
        assert(statsReceiver.stats(Seq("expired_ms")) == Nil)
        assert(await(res) == "polo")
      }
    }
  }

  test(
    "When the deadline is exceeded but the reject token bucket contains too few tokens, " +
      "DeadlineFilter should service the request and increment the exceeded stat"
  ) {
    Time.withCurrentTimeFrozen { tc =>
      val deadlineService = mkDeadlineService()
      Contexts.broadcast.let(Deadline, Deadline.ofTimeout(1.seconds)) {
        for (i <- 0 until 3) await(deadlineService("marco"))
        tc.advance(2.seconds)
        assert(await(deadlineService("marco")) == "polo")
        assert(statsReceiver.counters.get(List("exceeded")) == Some(1))
        assert(statsReceiver.counters(List("rejected")) == 0)
      }
    }
  }

  test(
    "When the deadline is exceeded and the reject token bucket contains sufficient tokens," +
      "DeadlineFilter should not service the request and increment the exceeded and rejected stats"
  ) {
    Time.withCurrentTimeFrozen { tc =>
      val deadlineService = mkDeadlineService()
      Contexts.broadcast.let(Deadline, Deadline.ofTimeout(1.seconds)) {
        for (i <- 0 until 5) await(deadlineService("marco"))
        tc.advance(2.seconds)
        val f = intercept[DeadlineExceededException] {
          await(deadlineService("marco"))
        }
        assert(f.getMessage.contains("Exceeded request deadline"))
        assert(statsReceiver.counters.get(List("exceeded")) == Some(1))
        assert(statsReceiver.counters.get(List("rejected")) == Some(1))
      }
    }
  }

  test(
    "When the deadline is exceeded and the reject token bucket contains sufficient tokens but we " +
      "are in dark mode, DeadlineFilter should service the request and increment the exceeded " +
      "and rejected stats"
  ) {
    val darkModeDeadlineFilter = new DeadlineFilter[String, String](
      rejectPeriod = 10.seconds,
      maxRejectFraction = 0.2,
      statsReceiver = statsReceiver,
      nowMillis = Stopwatch.timeMillis,
      isDarkMode = true
    )
    val darkModeService = darkModeDeadlineFilter.andThen(service)

    Time.withCurrentTimeFrozen { tc =>
      Contexts.broadcast.let(Deadline, Deadline.ofTimeout(1.seconds)) {
        for (i <- 0 until 5) await(darkModeService("marco"))
        tc.advance(2.seconds)
        assert(await(darkModeService("marco")) == "polo")
        assert(statsReceiver.counters.get(List("exceeded")) == Some(1))
        assert(statsReceiver.counters.get(List("rejected")) == Some(1))
      }
    }
  }

  test("tokens added to reject bucket on request without deadline") {
    Time.withCurrentTimeFrozen { tc =>
      val deadlineService = mkDeadlineService()
      for (i <- 0 until 5) await(deadlineService("marco"))

      Contexts.broadcast.let(Deadline, Deadline.ofTimeout(1.seconds)) {
        tc.advance(2.seconds)

        // 5 tokens should have been added, so we should be able to reject
        val f = intercept[DeadlineExceededException] {
          await(deadlineService("marco"))
        }
        assert(f.isFlagged(FailureFlags.DeadlineExceeded))
        assert(statsReceiver.counters.get(List("rejected")) == Some(1))
      }
    }
  }

  test(
    "tokens are added to bucket on request with expired deadline " +
      "when there are too few tokens to reject it"
  ) {
    Time.withCurrentTimeFrozen { tc =>
      val deadlineService = mkDeadlineService()
      Contexts.broadcast.let(Deadline, Deadline.ofTimeout(1.seconds)) {
        tc.advance(2.seconds)
        for (i <- 0 until 5) await(deadlineService("marco"))

        // 5 tokens should have been added, so we should be able to reject
        val f = intercept[DeadlineExceededException] {
          await(deadlineService("marco"))
        }
        assert(f.isFlagged(FailureFlags.DeadlineExceeded))
        assert(await(deadlineService("marco")) == "polo")
        assert(statsReceiver.counters.get(List("rejected")) == Some(1))
      }
    }
  }

  test("tokens not added to bucket when request is rejected") {
    Time.withCurrentTimeFrozen { tc =>
      val deadlineService = mkDeadlineService()
      Contexts.broadcast.let(Deadline, Deadline.ofTimeout(1.seconds)) {
        for (i <- 0 until 5) await(deadlineService("marco"))
        tc.advance(2.seconds)
        val f = intercept[DeadlineExceededException] {
          await(deadlineService("marco"))
        }
        assert(f.isFlagged(FailureFlags.DeadlineExceeded))
        assert(statsReceiver.counters.get(List("rejected")) == Some(1))
      }

      // If we add 4 more tokens, should still not be able to reject again.
      Contexts.broadcast.let(Deadline, Deadline.ofTimeout(1.seconds)) {
        for (i <- 0 until 4) await(deadlineService("marco"))
        tc.advance(2.seconds)
        await(deadlineService("marco"))
        assert(statsReceiver.counters.get(List("rejected")) == Some(1))
      }
    }
  }

  test("tokens added to reject bucket expire") {
    Time.withCurrentTimeFrozen { tc =>
      val deadlineService = mkDeadlineService()
      for (i <- 0 until 5) await(deadlineService("marco"))
      tc.advance(11.seconds)

      // tokens have expired so we should not be able to reject
      Contexts.broadcast.let(Deadline, Deadline.ofTimeout(1.seconds)) {
        tc.advance(2.seconds)
        await(deadlineService("marco"))
        assert(statsReceiver.counters(List("rejected")) == 0)
      }
    }
  }

  test("MaxRejectFraction param") {
    import DeadlineFilter._

    val p: MaxRejectFraction = MaxRejectFraction(0.5)

    val ps: Stack.Params = Stack.Params.empty + p
    assert(ps.contains[MaxRejectFraction])
    assert((ps[MaxRejectFraction] match { case MaxRejectFraction(d) => d }) == 0.5)
  }

  test("RejectPeriod param") {
    import DeadlineFilter._

    val p: RejectPeriod = RejectPeriod(5.seconds)

    val ps: Stack.Params = Stack.Params.empty + p
    assert(ps.contains[RejectPeriod])
    assert((ps[RejectPeriod] match { case RejectPeriod(d) => d }) == 5.seconds)
  }
}
