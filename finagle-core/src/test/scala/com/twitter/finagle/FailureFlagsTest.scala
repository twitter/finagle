package com.twitter.finagle

import java.util.concurrent.{Executors, ThreadPoolExecutor}
import com.twitter.conversions.DurationOps._
import com.twitter.util.{Await, Future, FuturePool, Throw}
import org.scalacheck.Gen
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.funsuite.AnyFunSuite

class FailureFlagsTest extends AnyFunSuite with ScalaCheckDrivenPropertyChecks {
  import FailureFlags._

  private def await[T](f: Future[T]): T =
    Await.result(f, 5.seconds)

  case class FlagCheck(flags: Long) extends FailureFlags[FlagCheck] {
    protected def copyWithFlags(f: Long): FlagCheck = FlagCheck(f)
  }

  private val flag = Gen.oneOf(
    FailureFlags.Empty,
    FailureFlags.Retryable,
    FailureFlags.Interrupted,
    FailureFlags.Wrapped,
    FailureFlags.Rejected,
    FailureFlags.Naming
  )
  // FailureFlags.NonRetryable - Conflicts with Restartable, so omitted here.

  private val flag2 = for (f1 <- flag; f2 <- flag if f1 != f2) yield f1 | f2

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
      FlagCheck(Interrupted | Retryable | Naming | Rejected | Wrapped),
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

  test("withFlags copies over stack trace, cause, suppressed") {
    val initial = FlagCheck(FailureFlags.Empty)
    initial.initCause(new RuntimeException("cause"))
    initial.addSuppressed(new IllegalArgumentException("suppressed1"))
    initial.addSuppressed(new UnsupportedOperationException("suppressed2"))

    val copied = initial.flagged(FailureFlags.ShowMask)

    assert(copied.getStackTrace.toSeq == initial.getStackTrace.toSeq)
    assert(copied.getCause == initial.getCause)
    assert(copied.getSuppressed.toSeq == initial.getSuppressed.toSeq)
  }

  test("FailureFlags.asNonRetryable removes retryable flag from FailureFlags exceptions") {
    val t = Throw(Failure.RetryableNackFailure)
    val asNonRetryable = intercept[FailureFlags[_]] {
      await(FailureFlags.asNonRetryable(t))
    }
    assert(!asNonRetryable.isFlagged(FailureFlags.Retryable))
  }

  test("FailureFlags.asNonRetryable on non-retryable FailureFlags exceptions") {
    val t = Throw(Failure.NonRetryableNackFailure)
    val asNonRetryable = intercept[FailureFlags[_]] {
      await(FailureFlags.asNonRetryable(t))
    }
    assert(!asNonRetryable.isFlagged(FailureFlags.Retryable))
  }

  test("FailureFlags.asNonRetryable does not modify non-FailureFlags exceptions") {
    val ex = new RuntimeException("not a FailureFlags")
    val t = Throw(ex)
    val notWrapped = intercept[RuntimeException] {
      await(FailureFlags.asNonRetryable(t))
    }
    assert(ex == notWrapped)
  }

  test("FuturePool doesn't obscure failure flags in interrupts") {
    val executor = Executors.newFixedThreadPool(1).asInstanceOf[ThreadPoolExecutor]
    val pool = FuturePool.interruptible(executor)
    val f = pool { await(Future.never) }
    f.raise(Failure.ignorable("hello"))
    val ff = intercept[FailureFlags[_]] { await(f) }
    assert(ff.isFlagged(FailureFlags.Ignorable))
  }
}
