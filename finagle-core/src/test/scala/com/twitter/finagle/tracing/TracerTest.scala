package com.twitter.finagle.tracing

import org.scalacheck.Gen
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.funsuite.AnyFunSuite
import scala.Float.NaN

class TracerTest extends AnyFunSuite with ScalaCheckDrivenPropertyChecks {

  case class TestTracer(res: Option[Boolean]) extends Tracer {
    def record(record: Record): Unit = ()
    def sampleTrace(traceId: TraceId): Option[Boolean] = res
    def getSampleRate: Float = NaN
    override def isActivelyTracing(traceId: TraceId): Boolean = res.getOrElse(true)
  }

  private val genNone: Gen[TestTracer] =
    Gen.const(TestTracer(None))

  private val genSomeTrue: Gen[TestTracer] =
    Gen.const(TestTracer(Some(true)))

  private val genSomeFalse: Gen[TestTracer] =
    Gen.const(TestTracer(Some(false)))

  private val genTracers: Gen[List[Tracer]] =
    Gen.listOf(Gen.oneOf(genNone, genSomeTrue, genSomeFalse))

  private val id = TraceId(None, None, SpanId(0L), None)

  test("BroadcastTracer.sampleTrace If all None returns None") {
    forAll(Gen.nonEmptyListOf(genNone)) { ts =>
      val bt = BroadcastTracer(ts)
      assert(bt.sampleTrace(id).isEmpty)
    }
  }

  test("BroadcastTracer.sampleTrace If one Some(true) returns Some(true)") {
    val gen = for {
      ts1 <- genTracers
      g <- genSomeTrue
      ts2 <- genTracers
    } yield (ts1 :+ g) ::: ts2

    forAll(gen) { ts =>
      val bt = BroadcastTracer(ts)
      assert(bt.sampleTrace(id).contains(true))
    }
  }

  test("BroadcastTracer.sampleTrace If one Some(false) returns None") {
    val gen = for {
      ts1 <- Gen.nonEmptyListOf(genNone)
      g <- genSomeFalse
      ts2 <- Gen.nonEmptyListOf(genNone)
    } yield (ts1 :+ g) ::: ts2

    forAll(gen) { ts =>
      val bt = BroadcastTracer(ts)
      assert(bt.sampleTrace(id).isEmpty)
    }
  }

  test("BroadcastTracer.sampleTrace If all Some(false) returns Some(false)") {
    forAll(Gen.nonEmptyListOf(genSomeFalse)) { ts =>
      val bt = BroadcastTracer(ts)
      assert(bt.sampleTrace(id).contains(false))
    }
  }

  test("BroadcastTracer.isActivelyTracing If any Some(true) returns true") {
    val gen = for {
      ts1 <- genTracers
      g <- genSomeTrue
      ts2 <- genTracers
    } yield (ts1 :+ g) ::: ts2

    forAll(gen) { ts =>
      val bt = BroadcastTracer(ts)
      assert(bt.isActivelyTracing(id))
    }
  }

  test("BroadcastTracer.isActivelyTracing If all Some(false) returns false") {
    val gen = for {
      ts1 <- Gen.listOf(genSomeFalse)
      g <- genSomeFalse
      ts2 <- Gen.listOf(genSomeFalse)
    } yield (ts1 :+ g) ::: ts2

    forAll(gen) { ts =>
      val bt = BroadcastTracer(ts)
      assert(!bt.isActivelyTracing(id))
    }
  }

  test("check equality of tracers") {
    val previous = DefaultTracer.self
    DefaultTracer.self = NullTracer

    val tracer = DefaultTracer
    assert(tracer == NullTracer, "Can't detect that tracer is NullTracer")

    DefaultTracer.self = ConsoleTracer
    assert(tracer != NullTracer, "Can't detect that tracer isn't NullTracer anymore")
    assert(tracer == ConsoleTracer, "Can't detect that tracer is ConsoleTracer")

    // Restore initial state
    DefaultTracer.self = previous
  }
}
