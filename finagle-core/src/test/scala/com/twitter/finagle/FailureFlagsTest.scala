package com.twitter.finagle

import org.junit.runner.RunWith
import org.scalacheck.Gen
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.prop.GeneratorDrivenPropertyChecks

@RunWith(classOf[JUnitRunner])
class FailureFlagsTest extends FunSuite with GeneratorDrivenPropertyChecks {
  import FailureFlags._

  case class FlagCheck(val flags: Long) extends FailureFlags[FlagCheck] {
    protected def copyWithFlags(f: Long): FlagCheck = FlagCheck(f)
  }

  private val flag = Gen.oneOf(
    0L,
    FailureFlags.Retryable,
    FailureFlags.Interrupted,
    FailureFlags.Wrapped,
    FailureFlags.Rejected,
    FailureFlags.Naming)
    // FailureFlags.NonRetryable - Conflicts with Restartable, so omitted here.

  private val flag2 = for (f1 <- flag; f2 <- flag if f1 != f2) yield f1|f2

  test("flagged, isFlagged, unflagged, masked") {
    for (flags <- Seq(flag, flag2)) {
      forAll(flags.suchThat(_ != 0)) { f =>
        assert(FlagCheck(f).isFlagged(f))
        assert(FlagCheck(Empty).flagged(f).flags == FlagCheck(f).flags)
        assert(FlagCheck(f).flags != FlagCheck(f).unflagged(f).flags)
        assert(!FlagCheck(Empty).isFlagged(f))
        assert(FlagCheck(f).masked(ShowMask).flags == (f & ShowMask))
      }
    }
  }

  test("FailureFlags.flagsOf") {
    val failures = Seq(
      FlagCheck(Interrupted|Retryable|Naming|Rejected|Wrapped),
      FlagCheck(NonRetryable),
      FlagCheck(Empty)
    )
    val names = Seq(
      Set("interrupted", "restartable", "wrapped", "rejected", "naming"),
      Set("nonretryable"),
      Set()
    )
    for ((f, n) <- failures.zip(names)) {
      assert(FailureFlags.flagsOf(f) == n)
    }
  }

  test("FailureFlags trait throws IllegalStateException when flagged with invalid combinations") {

    intercept[IllegalArgumentException] {
      FlagCheck(Retryable | NonRetryable)
    }
  }
}