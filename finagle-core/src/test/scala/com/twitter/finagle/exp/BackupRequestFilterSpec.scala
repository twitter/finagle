package com.twitter.finagle.exp

import com.twitter.conversions.time._
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.{Service, MockTimer}
import com.twitter.util.{Future, Promise, Time, Return, TimeControl}
import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito

class BackupRequestFilterSpec extends SpecificationWithJUnit with Mockito {
  "BackupRequestFilter" should {
    val statsReceiver = new InMemoryStatsReceiver
    val timer = new MockTimer
    class P extends Promise[String] {
      @volatile var interrupted: Throwable = null

      setInterruptHandler {
        case exc => interrupted = exc
      }
    }
    val p = new P
    val underlying = mock[Service[String, String]]
    underlying.close(any) returns Future.Done
    underlying(any) returns p
    val service = new BackupRequestFilter(10.milliseconds, timer, statsReceiver) andThen underlying

    "no backup requests when response arrives on time" in Time.withCurrentTimeFrozen { tc =>
      there was no(underlying)(any)
      timer.tasks must beEmpty
      val f = service("ok")
      there was one(underlying)("ok")
      f.isDefined must beFalse
      timer.tasks must haveSize(1)
      p.setValue("ok")
      f.poll must beSome(Return("ok"))
      timer.tasks must beEmpty
      statsReceiver.counters must havePair(Seq("backup", "won") -> 1)
      statsReceiver.counters must notHaveKey(Seq("backup", "timeouts"))
    }

    class BackupSpec(tc: TimeControl) {
      val f = service("ok")
      f.isDefined must beFalse
      timer.tasks must haveSize(1)
      there was one(underlying)("ok")
      val p1 = new P
      underlying(any) returns p1
      tc.advance(10.milliseconds)
      timer.tick()
      there were two(underlying)("ok")
      f.isDefined must beFalse
    }

    "issue backup requests when service does not respond in time" in Time.withCurrentTimeFrozen { tc =>
      new BackupSpec(tc)
      statsReceiver.counters must havePair(Seq("backup", "timeouts") -> 1)
    }

    "when original wins, choose it and interrupt backup" in Time.withCurrentTimeFrozen { tc =>
      val b = new BackupSpec(tc)
      import b._
      p1.interrupted must beNull
      p.setValue("yay")
      f.poll must beSome(Return("yay"))
      p1.interrupted must be_==(BackupRequestLost)
      statsReceiver.counters must havePair(Seq("backup", "timeouts") -> 1)
      statsReceiver.counters must havePair(Seq("backup", "won") -> 1)
    }

    "when backup wins, choose it and cancel original" in Time.withCurrentTimeFrozen { tc =>
      val b = new BackupSpec(tc)
      import b._
      p.interrupted must beNull
      p1.setValue("okay!")
      f.poll must beSome(Return("okay!"))
      p.interrupted must be_==(BackupRequestLost)
      statsReceiver.counters must havePair(Seq("backup", "timeouts") -> 1)
      statsReceiver.counters must havePair(Seq("backup", "lost") -> 1)
    }
  }
}
