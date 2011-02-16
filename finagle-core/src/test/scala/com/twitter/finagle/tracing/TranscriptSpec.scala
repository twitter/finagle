package com.twitter.finagle.tracing

import org.specs.Specification

import com.twitter.conversions.time._

import com.twitter.util.Time

object TrascriptSpec extends Specification {
  "BufferingTranscript" should {
    val traceID = TraceID(1L, Some(2L), 0, "myVM")

    "record traceID, current time, and message" in {
      Time.withCurrentTimeFrozen { timeControl =>
        val t = new BufferingTranscript(traceID)
        t.record("hey there")

        val expectedRecord = Record(traceID, Time.now, "hey there")

        t.size must be_==(1)
        t.head must be_==(expectedRecord)
      }
    }

    "merge" in {
      Time.withCurrentTimeFrozen { timeControl =>
        val t0 = new BufferingTranscript(traceID)
        val t1 = new BufferingTranscript(traceID)

        t0.record("1")
        timeControl.advance(1.second)
        t1.record("2")
        timeControl.advance(1.second)
        t0.record("3")

        t0.merge(t1.iterator)
        t0 must haveSize(3)
        val records = t0.toArray
        records(0).message must be_==("1")
        records(1).message must be_==("2")
        records(2).message must be_==("3")

        // Merging again should kill dups:
        t0.merge(t1.iterator)
        t0 must haveSize(3)
      }
    }
  }
}
