package com.twitter.finagle.context

import com.twitter.finagle.service.DeadlineOnlyToggle
import com.twitter.util.Time
import com.twitter.util.Duration
import com.twitter.util.Return
import org.scalacheck.Gen
import org.scalatestplus.junit.AssertionsForJUnit
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.funsuite.AnyFunSuite

class DeadlineTest extends AnyFunSuite with AssertionsForJUnit with ScalaCheckDrivenPropertyChecks {

  val time = for (t <- Gen.choose(0L, Long.MaxValue)) yield Time.fromNanoseconds(t)
  val dur = for (d <- Gen.choose(0L, Long.MaxValue)) yield Duration.fromNanoseconds(d)
  val deadline = for (t <- time; d <- dur) yield Deadline(t, t + d)
  val deadlineWithoutTop = deadline.filter(_.deadline != Time.Top)

  test("Deadline marshalling") {
    // won't pass Time.Top as deadline for marshalling
    forAll(deadlineWithoutTop) { d =>
      assert(Deadline.tryUnmarshal(Deadline.marshal(d)) == Return(d))
    }
  }

  test("Deadline.currentToggled") {
    val sampled = deadline.sample.get

    Contexts.broadcast.let(Deadline, sampled) {
      assert(Deadline.current == Some(sampled))
      assert(Deadline.currentToggled == None)

      DeadlineOnlyToggle.unsafeOverride(Some(true))
      try assert(Deadline.currentToggled == Some(sampled))
      finally DeadlineOnlyToggle.unsafeOverride(None)
    }
  }

  test("Deadline.combined") {
    forAll(deadline, deadline) { (d1, d2) =>
      assert(Deadline.combined(d1, d2).timestamp == (d1.timestamp max d2.timestamp))
      assert(Deadline.combined(d1, d2).deadline == (d1.deadline min d2.deadline))
      assert(Deadline.combined(d1, d2) == Deadline.combined(d2, d1))
    }
  }
}
