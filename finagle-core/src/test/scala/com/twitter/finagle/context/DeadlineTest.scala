package com.twitter.finagle.context

import com.twitter.util.{Time, Duration, Return}
import org.junit.runner.RunWith
import org.scalacheck.Gen
import org.scalatest.FunSuite
import org.scalatest.junit.{AssertionsForJUnit, JUnitRunner}
import org.scalatest.prop.GeneratorDrivenPropertyChecks

@RunWith(classOf[JUnitRunner])
class DeadlineTest 
  extends FunSuite 
  with AssertionsForJUnit
  with GeneratorDrivenPropertyChecks {

  val time = for (t <- Gen.choose(0L, Long.MaxValue)) yield Time.fromNanoseconds(t)
  val dur = for (d <- Gen.choose(0L, Long.MaxValue)) yield Duration.fromNanoseconds(d)
  val deadline = for (t <- time; d <- dur) yield Deadline(t, t + d)
  val deadlineWithoutTop  = deadline.filter(_.deadline != Time.Top)

  test("Deadline marshalling") {
    // won't pass Time.Top as deadline for marshalling
    forAll(deadlineWithoutTop) { d =>
      assert(Deadline.tryUnmarshal(Deadline.marshal(d)) == Return(d))
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

