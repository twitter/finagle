package com.twitter.finagle

import com.twitter.conversions.DurationOps._
import com.twitter.logging.{HasLogLevel, Level}
import com.twitter.util.{Await, Future}
import org.scalacheck.Gen
import org.scalatestplus.junit.AssertionsForJUnit
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.funsuite.AnyFunSuite

class FailureTest extends AnyFunSuite with AssertionsForJUnit with ScalaCheckDrivenPropertyChecks {
  private val exc = Gen.oneOf[Throwable](null, new Exception("first"), new Exception("second"))

  private val flag = Gen.oneOf(
    0L,
    FailureFlags.Retryable,
    FailureFlags.Interrupted,
    FailureFlags.Wrapped,
    FailureFlags.Rejected,
    FailureFlags.Naming
  )
  // FailureFlags.NonRetryable - Conflicts with Restartable, so omitted here.

  private val flag2 = for (f1 <- flag; f2 <- flag if f1 != f2) yield f1 | f2

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

      assert(Failure(e1, f) == Failure(e1, f))
      assert(Failure(e1, f) != Failure(e2, f))
      assert(Failure(e1, f).hashCode == Failure(e1, f).hashCode)
    }
  }

  test("flags") {
    val e = new Exception

    for (flags <- Seq(flag, flag2)) {
      forAll(flags.suchThat(_ != 0)) { f =>
        assert(Failure(e, f).isFlagged(f))
        assert(Failure(e).flagged(f) == Failure(e, f))
        assert(Failure(e, f) != Failure(e, f).unflagged(f))
        assert(Failure(e, f).isFlagged(f))
        assert(!Failure(e, 0).isFlagged(f))
      }
    }
  }

  test("Failure.adapt(Failure)") {
    val parent = Failure("sadface", FailureFlags.Retryable)

    val f = Failure.adapt(parent, FailureFlags.Interrupted)
    assert(f.flags == (FailureFlags.Retryable | FailureFlags.Interrupted))
    assert(f.getCause == parent)
    assert(f.getMessage == "sadface")
    assert(f != parent)
  }

  test("Failure.adapt(Throwable)") {
    val parent = new Exception("sadface")

    val f = Failure.adapt(parent, FailureFlags.Interrupted)
    assert(f.flags == FailureFlags.Interrupted)
    assert(f.isFlagged(FailureFlags.Interrupted))
    assert(f.getCause == parent)
    assert(f.getMessage == "sadface")
    assert(f != parent)
  }

  test("Failure.show") {
    assert(
      Failure(
        "ok",
        FailureFlags.Rejected | FailureFlags.Retryable | FailureFlags.Interrupted).show == Failure(
        "ok",
        FailureFlags.Interrupted | FailureFlags.Rejected
      )
    )
    val inner = new Exception
    assert(Failure.wrap(inner).show == inner)
    assert(Failure.wrap(Failure.wrap(inner)).show == inner)
  }

  test("Invalid Failure") {
    intercept[IllegalArgumentException] {
      Failure(null: Throwable, FailureFlags.Wrapped)
    }
  }

  test("Invalid flag combinations") {
    intercept[IllegalArgumentException] {
      Failure("eh", FailureFlags.NonRetryable | FailureFlags.Retryable)
    }
  }

  private class WithLogLevel(val logLevel: Level) extends Exception with HasLogLevel

  test("Failure.apply uses HasLogLevel's logLevel") {
    Seq(Level.CRITICAL, Level.TRACE, Level.ALL).foreach { level =>
      assert(level == Failure(new WithLogLevel(level)).logLevel)
    }
  }

  test("Failure.apply follows chain to HasLogLevel") {
    val nested = new Exception(new WithLogLevel(Level.DEBUG))
    assert(Level.DEBUG == Failure(nested).logLevel)
  }

  test("Failure.apply defaults logLevel to warning") {
    assert(Level.WARNING == Failure(new Exception()).logLevel)
  }

  test("Failure.rejected sets correct flags") {
    val flags = FailureFlags.Retryable | FailureFlags.Rejected
    assert(Failure.rejected(":(").isFlagged(flags))
    assert(Failure.rejected(":(", new Exception).isFlagged(flags))
    assert(Failure.rejected(new Exception).isFlagged(flags))
  }

  test("Failure.ProcessFailures") {
    val echo = Service.mk((exc: Throwable) => Future.exception(exc))
    val service = new Failure.ProcessFailures().andThen(echo)

    def assertFail(exc: Throwable, expect: Throwable) = {
      val exc1 = intercept[Throwable] { Await.result(service(exc), 5.seconds) }
      assert(exc1 == expect)
    }

    assertFail(Failure("ok", FailureFlags.Retryable), Failure("ok"))
    assertFail(Failure("ok"), Failure("ok"))
    assertFail(Failure("ok", FailureFlags.Interrupted), Failure("ok", FailureFlags.Interrupted))
    assertFail(
      Failure("ok", FailureFlags.Interrupted | FailureFlags.Retryable),
      Failure("ok", FailureFlags.Interrupted)
    )
    assertFail(
      Failure("ok", FailureFlags.Rejected | FailureFlags.NonRetryable),
      Failure("ok", FailureFlags.Rejected | FailureFlags.NonRetryable)
    )

    val inner = new Exception
    assertFail(Failure.wrap(inner), inner)
  }

  test("Failure.flagsOf") {
    val failures = Seq(
      Failure(
        "abc",
        new Exception,
        FailureFlags.Interrupted | FailureFlags.Retryable | FailureFlags.Naming | FailureFlags.Rejected | FailureFlags.Wrapped
      ),
      Failure("abc", FailureFlags.NonRetryable),
      Failure("abc"),
      new Exception
    )
    val categories = Seq(
      Set("interrupted", "restartable", "wrapped", "rejected", "naming"),
      Set("nonretryable"),
      Set(),
      Set()
    )
    for ((f, c) <- failures.zip(categories)) {
      assert(FailureFlags.flagsOf(f) == c)
    }
  }

  test("Failure.wrap(non-wrapped Failure)") {
    val ex1 = Failure("Not wrapped.")
    val ex2 = Failure.wrap(ex1, FailureFlags.Naming)
    assert(ex2.flags == FailureFlags.Naming)
  }

  test("Failure.retryable(..) strips NonRetryable flag") {
    val ex = Failure("Not retryable", FailureFlags.NonRetryable)
    val result = Failure.retryable(ex)

    assert(result.isFlagged(FailureFlags.Retryable))
    assert(!result.isFlagged(FailureFlags.NonRetryable))
  }
}
