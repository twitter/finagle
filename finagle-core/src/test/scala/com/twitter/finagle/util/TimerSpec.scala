package com.twitter.finagle.util

import org.specs.Specification
import org.specs.mock.Mockito

import java.util.concurrent.TimeUnit
import org.jboss.netty.{util => nu}
import com.twitter.util.{
  CountDownLatch, Time, TimerTask,
  ReferenceCountingTimer, MockTimer}
import com.twitter.conversions.time._

object TimerSpec extends Specification with Mockito {
  "Timer" should {
    val timer = Timer.default
    timer.acquire()
    doAfter { timer.stop() }

    val start = Time.now
    var end = Time.now
    val latch = new CountDownLatch(1)
    val task = timer.schedule(1.second.fromNow) {
      latch.countDown()
      end = Time.now
    }

    "not execute the task until it has timed out" in {
      latch.await(2.seconds) must beTrue
      (end - start).moreOrLessEquals(1.second, 10.milliseconds)
    }

    "not execute the task if it has been cancelled" in {
      task.cancel()
      latch.await(2.seconds) must beFalse
    }
  }
}

object TimerToNettyTimerSpec extends Specification with Mockito {
  // We have to jump through a lot of hoops here just
  // to make assertions about Timer#schedule calls.
  class MockReferenceCountingTimer(underlying: Timer)
    extends ReferenceCountingTimer(() => underlying)
  {
    val scheduled =
      new collection.mutable.ArrayBuffer[(Time, () => Unit, TimerTask)]
    override def schedule(when: Time)(f: => Unit) = {
      val tt = mock[TimerTask]
      scheduled.append((when, () => f, tt))
      tt
    }
  }

  "TimerToNettyTimer" should {
    val underlyingTimer = mock[Timer]
    val underlying = spy(
      new MockReferenceCountingTimer(underlyingTimer))
    val nettyTimer = new TimerToNettyTimer(underlying)
    var ran: Option[nu.Timeout] = None

    def schedule() = nettyTimer.newTimeout(
      new nu.TimerTask {
        def run(to: nu.Timeout) {
          ran = Some(to)
        }
      }, 10, TimeUnit.SECONDS)


    "Adding a timeout" in {
      there was no(underlying).acquire()
      underlying.scheduled must beEmpty

      "schedule on underlying timer" in Time.withCurrentTimeFrozen { tc =>
        schedule()
        there was one(underlying).acquire()
        underlying.scheduled must haveSize(1)
        underlying.scheduled(0) must beLike {
          case (t, _, _) if t == 10.seconds.fromNow => true
        }
        ran must beNone
        there was no(underlying).stop()
      }

      "when running" in {
        "dereference the timer" in {
          schedule()
          val (_, f, _) = underlying.scheduled(0)
          f()
          there was one(underlying).stop()
        }

        "set expired on the task" in {
          val task = schedule()
          task.isExpired must beFalse
          val (_, f, _) = underlying.scheduled(0)
          f()
          task.isExpired must beTrue
        }
      }

      "when cancelling" in {
        "cancel the underlying task" in {
          val task = schedule()
          val (_, _, tt) = underlying.scheduled(0)
          there was no(tt).cancel()
          task.cancel()
          there was one(tt).cancel()
          there was one(underlying).stop()
        }

        "sets cancelled/expired on task" in {
          val task = schedule()
          task.isCancelled must beFalse
          task.cancel()
          task.isCancelled must beTrue
          task.isExpired must beTrue
        }
      }
    }
  }

  "CountingTimer" should {
    val underlying = new MockTimer
    val timer = new CountingTimer(underlying)
    "count when run" in Time.withCurrentTimeFrozen { tc =>
      timer.count must be_==(0)
      1 until 100 foreach { i =>
        timer.schedule(10.seconds.fromNow) {/*nada*/}
        timer.count must be_==(i)
      }
      tc.advance(10.seconds)
      underlying.tick()
      timer.count must be_==(0)
    }

    "count when cancelled" in Time.withCurrentTimeFrozen { tc =>
      timer.count must be_==(0)
      val tasks = 1 until 100 map { i =>
        val t = timer.schedule(10.seconds.fromNow) {/*nada*/}
        timer.count must be_==(i)
        t
      }

      tasks.zipWithIndex.reverse foreach { case (t, i) =>
        t.cancel()
        timer.count must be_==(i)
      }
    }

    "run when ran" in Time.withCurrentTimeFrozen { tc =>
      var ran = 0
      1 until 100 foreach { i =>
        timer.schedule(i.seconds.fromNow) {ran += 1}
        timer.count must be_==(i)
      }
      1 until 100 foreach { i =>
        tc.advance(1.second)
        underlying.tick()
        ran must be_==(i)
      }
      timer.count must be_==(0)
    }
  }
}
