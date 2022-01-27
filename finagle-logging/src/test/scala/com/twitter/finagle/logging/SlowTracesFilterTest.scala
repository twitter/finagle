package com.twitter.finagle.logging

import com.twitter.conversions.DurationOps._
import com.twitter.conversions.PercentOps._
import com.twitter.finagle.Service
import com.twitter.finagle.tracing.BufferingTracer
import com.twitter.finagle.tracing.Trace
import com.twitter.util.Await
import com.twitter.util.Future
import com.twitter.util.MockTimer
import com.twitter.util.Time
import com.twitter.util.Timer
import org.scalatest.funsuite.AnyFunSuite

class SlowTracesFilterTest extends AnyFunSuite {
  class Debugger extends (String => Unit) {
    private[this] var seq: Seq[String] = Seq.empty

    def apply(record: String): Unit = {
      seq :+= record
    }

    def logs: Seq[String] = seq
  }

  private[this] def svc(timer: Timer) = Service.mk[Int, Int] { num: Int =>
    Future.sleep(num.millis)(timer).before(Future.value(0))
  }

  private[this] def await[A](a: Future[A]): A = Await.result(a, 5.seconds)

  private[this] def mkLog(latency: Int, measured: Int): String =
    s"Detected very slow request that's sampled by tracing. 0.5 latency ${latency}ms, measured request latency ${measured}ms, Trace ID"

  test("SlowTracesFilter samples on the slow requests") {
    val debugger = new Debugger
    Time.withCurrentTimeFrozen { ctl =>
      val timer = new MockTimer()
      val filter = new SlowTracesFilter(50.percent, debugger, timer).toFilter[Int, Int]
      val filtered = filter.andThen(svc(timer))

      def mkRequest(num: Int): Unit = {
        val f = filtered(num)
        ctl.advance(num.millis)
        timer.tick()
        await(f)
      }
      Trace.letTracer(new BufferingTracer) {
        for { i <- 0 until 100 } {
          mkRequest(i)
        }
        ctl.advance(10.seconds)
        timer.tick()
        mkRequest(50)
        val snipped = debugger.logs.map { log =>
          val idx = log.indexOf("ID")
          log.substring(0, idx + 2)
        }
        assert(snipped == Seq((mkLog(49, 50))))
      }
    }
  }

  test("SlowTracesFilter doesn't sample if it's not traced") {
    val debugger = new Debugger
    Time.withCurrentTimeFrozen { ctl =>
      val timer = new MockTimer()
      val filter = new SlowTracesFilter(50.percent, debugger, timer).toFilter[Int, Int]
      val filtered = filter.andThen(svc(timer))

      def mkRequest(num: Int): Unit = {
        val f = filtered(num)
        ctl.advance(num.millis)
        timer.tick()
        await(f)
      }
      for { i <- 0 until 100 } {
        mkRequest(i)
      }
      ctl.advance(10.seconds)
      timer.tick()
      mkRequest(50)
      assert(debugger.logs.isEmpty)
    }
  }

  test("SlowTracesFilter doesn't sample if it's too low") {
    val debugger = new Debugger
    Time.withCurrentTimeFrozen { ctl =>
      val timer = new MockTimer()
      val filter = new SlowTracesFilter(50.percent, debugger, timer).toFilter[Int, Int]
      val filtered = filter.andThen(svc(timer))

      def mkRequest(num: Int): Unit = {
        val f = filtered(num)
        ctl.advance(num.millis)
        timer.tick()
        await(f)
      }
      for { i <- 0 until 100 } {
        mkRequest(i)
      }
      ctl.advance(10.seconds)
      timer.tick()
      mkRequest(49)
      assert(debugger.logs.isEmpty)
    }
  }
}
