package com.twitter.finagle.service

import com.twitter.finagle._
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.tracing._
import com.twitter.util._
import com.twitter.util.TimeConversions._
import org.junit.runner.RunWith
import org.mockito.{ArgumentCaptor, Matchers}
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import scala.collection.JavaConverters._


@RunWith(classOf[JUnitRunner])
class DeadlineStatsFilterTest extends FunSuite with MockitoSugar {

  class DeadlineFilterHelper {
    val timer = new MockTimer
    val promise = new Promise[String]
    val service = new Service[String, String] {
      def apply(request: String) = promise
    }
    val statsReceiver = new InMemoryStatsReceiver
    val deadlineFilter = new DeadlineStatsFilter[String, String](statsReceiver)
    val deadlineService = deadlineFilter.andThen(service)
  }

  test("DeadlineFilter should service the request") {
    val h = new DeadlineFilterHelper
    import h._

    promise.setValue("polo")

    val res = deadlineService("marco")
    assert(statsReceiver.counters.get(List("exceeded")) == None)
    assert(Await.result(res, 1.second) == "polo")
  }


  test("DeadlineFilter should record transit time for the request") {
    val h = new DeadlineFilterHelper
    import h._

    promise.setValue("polo")

    Time.withCurrentTimeFrozen { tc =>
      Contexts.broadcast.let(Deadline, Deadline.ofTimeout(1.seconds)) {
        tc.advance(200.milliseconds)
        val res = deadlineService("marco")
        assert(statsReceiver.stats(Seq("transit_latency_ms"))(0) == 200f)
        assert(Await.result(res, 1.second) == "polo")
      }
    }
  }

  test("DeadlineFilter should record remaining deadline for the request") {
    val h = new DeadlineFilterHelper
    import h._

    promise.setValue("polo")

    Time.withCurrentTimeFrozen { tc =>
      Contexts.broadcast.let(Deadline, Deadline.ofTimeout(1.seconds)) {
        tc.advance(200.milliseconds)
        val res = deadlineService("marco")
        assert(statsReceiver.stats(Seq("deadline_budget_ms"))(0) == 800f)
        assert(Await.result(res, 1.second) == "polo")
      }
    }
  }

  test("When the deadline is exceeded DeadlineFilter should increment the " +
      "exceeded stat") {
    val h = new DeadlineFilterHelper
    import h._

    promise.setValue("polo")

    Time.withCurrentTimeFrozen { tc =>
      Contexts.broadcast.let(Deadline, Deadline.ofTimeout(1.seconds)) {
        tc.advance(2.seconds)
        assert(Await.result(deadlineService("marco"), 1.second) == "polo")
        assert(statsReceiver.counters.get(List("exceeded")) == Some(1))
      }
    }
  }

}
