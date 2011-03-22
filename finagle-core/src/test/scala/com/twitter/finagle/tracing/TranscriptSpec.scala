package com.twitter.finagle.tracing

import org.specs.Specification

import com.twitter.conversions.time._

import com.twitter.util.Time

object TrascriptSpec extends Specification {
  "BufferingTranscript" should {
    "record traceID, current time, and message" in {
      Time.withCurrentTimeFrozen { timeControl =>
        val t = new BufferingTranscript
        t.record(Annotation.Message("hey there"))

        val expectedRecord = Record(Time.now, Annotation.Message("hey there"))

        t.size must be_==(1)
        t.head must be_==(expectedRecord)
      }
    }

    "recordAll" in {
      Time.withCurrentTimeFrozen { timeControl =>
        val t0 = new BufferingTranscript
        val t1 = new BufferingTranscript

        t0.record(Annotation.Message("1"))
        timeControl.advance(1.second)
        t1.record(Annotation.Message("2"))
        timeControl.advance(1.second)
        t0.record(Annotation.Message("3"))

        t0.recordAll(t1.iterator)
        t0 must haveSize(3)
        val records = t0.toArray
        records(0).annotation must be_==(Annotation.Message("1"))
        records(1).annotation must be_==(Annotation.Message("2"))
        records(2).annotation must be_==(Annotation.Message("3"))

        // Merging again should kill dups:
        t0.recordAll(t1.iterator)
        t0 must haveSize(3)
      }
    }
  }
}
