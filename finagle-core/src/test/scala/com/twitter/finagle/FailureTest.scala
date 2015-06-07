package com.twitter.finagle

import com.twitter.util.{Await, Future}
import org.junit.runner.RunWith
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.FunSuite
import org.scalatest.junit.{JUnitRunner, AssertionsForJUnit}
import org.scalatest.prop.GeneratorDrivenPropertyChecks

@RunWith(classOf[JUnitRunner])
class FailureTest extends FunSuite with AssertionsForJUnit with GeneratorDrivenPropertyChecks {
  val exc = Gen.oneOf[Throwable](
    null,
    new Exception("first"),
    new Exception("second"))

  val flag = Gen.oneOf(
    0L,
    Failure.Restartable,
    Failure.Interrupted,
    Failure.Wrapped,
    Failure.Naming)

  val flag2 = for (f1 <- flag; f2 <- flag if f1 != f2) yield f1|f2

  test("simple failures with a cause") {
    val why = "boom!"
    val exc = new Exception(why)
    val failure = Failure(exc)
    val Failure(Some(`exc`)) = failure
    assert(failure.why == why)
  }

  test("equality") {
    forAll(flag2) { f =>
      val e1, e2 = new Exception

      Failure(e1, f) == Failure(e1, f) &&
      Failure(e1, f) != Failure(e2, f) &&
      Failure(e1, f) != Failure(e1, ~f) &&
      Failure(e1, f).hashCode == Failure(e1, f).hashCode
    }
  }

  test("flags") {
    val e = new Exception

    for (flags <- Seq(flag, flag2)) {
      forAll(flags.suchThat(_!=0)) { f =>
        Failure(e, f).isFlagged(f) &&
        Failure(e).flagged(f) == Failure(e, f) &&
        Failure(e, f) != Failure(e, f).unflagged(f) &&
        Failure(e, f).isFlagged(f) &&
        !Failure(e, 0).isFlagged(f)
      }
    }
  }

  test("Failure.adapt(Failure)") {
    val parent = Failure("sadface", Failure.Restartable)

    val f = Failure.adapt(parent, Failure.Interrupted)
    assert(f.flags == (Failure.Restartable|Failure.Interrupted))
    assert(f.getCause == parent)
    assert(f.getMessage == "sadface")
    assert(f != parent)
  }

  test("Failure.adapt(Throwable)") {
    val parent = new Exception("sadface")

    val f = Failure.adapt(parent, Failure.Interrupted)
    assert(f.flags == Failure.Interrupted)
    assert(f.isFlagged(Failure.Interrupted))
    assert(f.getCause == parent)
    assert(f.getMessage == "sadface")
    assert(f != parent)
  }

  test("Failure.show") {
    assert(Failure("ok", Failure.Restartable|Failure.Interrupted).show == Failure("ok", Failure.Interrupted))
    val inner = new Exception
    assert(Failure.wrap(inner).show == inner)
    assert(Failure.wrap(Failure.wrap(inner)).show == inner)
  }

  test("Invalid Failure") {
    intercept[IllegalArgumentException] {
      Failure(null: Throwable, Failure.Wrapped)
    }
  }

  test("Failure.ProcessFailures") {
    val echo = Service.mk((exc: Throwable) => Future.exception(exc))
    val service = (new Failure.ProcessFailures) andThen echo

    def assertFail(exc: Throwable, expect: Throwable) = {
      val exc1 = intercept[Throwable] { Await.result(service(exc)) }
      assert(exc1 == expect)
    }

    assertFail(Failure("ok", Failure.Restartable), Failure("ok"))

    assertFail(Failure("ok"), Failure("ok"))
    assertFail(Failure("ok", Failure.Interrupted), Failure("ok", Failure.Interrupted))
    assertFail(Failure("ok", Failure.Interrupted|Failure.Restartable), Failure("ok", Failure.Interrupted))

    val inner = new Exception
    assertFail(Failure.wrap(inner), inner)
  }

  test("Failure.flagsOf") {
    val failures = Seq(
      Failure("abc", new Exception, Failure.Interrupted|Failure.Restartable|Failure.Naming|Failure.Wrapped),
      Failure("abc"),
      new Exception
    )
    val categories = Seq(
      Set("interrupted", "restartable", "wrapped", "naming"),
      Set(),
      Set()
    )
    for ((f, c) <- failures.zip(categories)) {
      assert(Failure.flagsOf(f) == c)
    }
  }
}

