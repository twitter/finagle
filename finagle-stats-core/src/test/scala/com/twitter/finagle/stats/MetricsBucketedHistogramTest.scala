package com.twitter.finagle.stats

import com.twitter.conversions.DurationOps._
import com.twitter.util.{Duration, Time, TimeControl}
import org.scalatest.funsuite.AnyFunSuite

class MetricsBucketedHistogramTest extends AnyFunSuite {

  // use an arbitrary time that will not fall into
  // the next snap window while the test does `roll()`s.
  val BeginningOfMinute = Time.fromSeconds(1490332920)

  test("basics") {
    // use an arbitrary time that will not fall into
    // the next snap window while the test does `roll()`s.
    Time.withTimeAt(Time.fromSeconds(1439242122)) { tc =>
      val ps = Array[Double](0.5, 0.9)
      val h = new MetricsBucketedHistogram(name = "h", percentiles = ps)

      def roll(): Unit = {
        tc.advance(60.seconds)
      }

      // add some data (A) to the 1st window
      1L.to(100L).foreach(h.add)

      // since we have not rolled to the next window, we should not see that data
      val snap0 = h.snapshot
      withClue(snap0) {
        assert(snap0.min == 0, snap0)
        assert(snap0.max == 0, snap0)
        assert(snap0.count == 0, snap0)
        assert(snap0.sum == 0, snap0)
        assert(snap0.average == 0, snap0)
        assert(snap0.percentiles.map(_.value) === Array(0, 0))
      }

      // roll to window 2 (this should make data A visible after a call to snapshot)
      roll()
      val snap1 = h.snapshot
      withClue(snap1) {
        assert(snap1.min == 1)
        assert(snap1.max == 100)
        assert(snap1.count == 100)
        assert(snap1.sum == 1.to(100).sum)
        assert(snap1.average == 50.5d)
        assert(snap1.percentiles.map(_.value) === Array(50, 90))
      }

      // add a data point (B) to window 2 (it will not be visible in the snapshot)
      h.add(1000)
      assert(h.snapshot.sum == snap1.sum, snap1)

      // fill out this 2nd window (C) and roll the window, we should only see B and C
      1001L.to(10000L).foreach(h.add)
      roll()
      val snap2 = h.snapshot
      withClue(snap2) {
        assert(snap2.min == 1003) // this only needs to be +/- 0.5%
        assert(snap2.max == 9987) // this only needs to be +/- 0.5%
        assert(snap2.count == 9001)
        assert(snap2.sum == 1000L.to(10000L).sum)
        assert(snap2.average == 5500.0)
        assert(snap2.percentiles.map(_.value) === Array(5498, 9132))
      }

      // roll to the next window, which should evict B and C as well
      roll()
      val snap3 = h.snapshot
      withClue(snap3) {
        assert(snap3.min == 0L)
        assert(snap3.max == 0L)
        assert(snap3.count == 0L)
        assert(snap3.sum == 0L)
        assert(snap3.average == 0.0)
        assert(snap3.percentiles.map(_.value) === Array(0, 0))
      }

      // add some data (D), roll it into view then confirm clear works
      h.add(1)
      roll()
      assert(h.snapshot.count == 1L)
      h.clear()
      val snap4 = h.snapshot
      withClue(snap4) {
        assert(snap4.min == 0)
        assert(snap4.max == 0)
        assert(snap4.count == 0)
        assert(snap4.sum == 0)
        assert(snap4.average == 0)
        assert(snap4.percentiles.map(_.value) === Array(0, 0))
      }
    }
  }

  class Ctx(tc: TimeControl, latchPeriod: Duration = 1.minute) {
    val h = new MetricsBucketedHistogram(name = "h", latchPeriod = latchPeriod)
    val details = h.histogramDetail

    def roll(duration: Duration = latchPeriod): Unit = {
      tc.advance(duration)
      h.snapshot
    }
  }

  test("histogram snapshot is available on first request") {
    Time.withCurrentTimeFrozen { tc =>
      val ctx = new Ctx(tc)
      import ctx._

      // add some data (A) to the 1st window
      h.add(1)

      // initial user access to start histogram snapshots
      val init = details.counts
      assert(init == Seq(BucketAndCount(1, 2, 1)))
    }
  }

  test("histogram snapshot respects refresh window") {
    Time.withTimeAt(BeginningOfMinute) { tc =>
      val ctx = new Ctx(tc)
      import ctx._

      // add some data (A) to the 1st window
      h.add(1)

      // initial user access to start histogram snapshots
      assert(details.counts == Seq(BucketAndCount(1, 2, 1)))

      // call .snapshot() to recompute counts
      h.snapshot
      assert(details.counts == Seq(BucketAndCount(1, 2, 1)))

      h.add(Int.MaxValue)
      // roll to window 2 (this should make data A visible after a call to snapshot)
      roll()
      assert(
        details.counts == Seq(BucketAndCount(1, 2, 1), BucketAndCount(2137204091, Int.MaxValue, 1))
      )
    }
  }

  test("histogram snapshot stays up-to-date when snapshots are missed") {
    Time.withTimeAt(BeginningOfMinute) { tc =>
      val ctx = new Ctx(tc)
      import ctx._

      h.add(1)
      roll()
      assert(details.counts == Seq(BucketAndCount(1, 2, 1)))

      // Advance several minutes without snapshotting (i.e. to
      // simulate no reads). Subsequent reads should be stable.
      h.add(2)
      tc.advance(1.minute)
      h.add(3)
      tc.advance(1.minute)
      h.add(4)
      tc.advance(1.minute)
      h.add(5)
      tc.advance(1.minute)
      h.snapshot
      val expectedCounts = Seq(
        BucketAndCount(1, 2, 1),
        BucketAndCount(2, 3, 1),
        BucketAndCount(3, 4, 1),
        BucketAndCount(4, 5, 1),
        BucketAndCount(5, 6, 1)
      )
      assert(details.counts == expectedCounts)

      h.add(6)
      tc.advance(1.second)
      h.snapshot
      assert(details.counts == expectedCounts)

      h.add(7)
      tc.advance(1.second)
      h.snapshot
      assert(details.counts == expectedCounts)

      h.add(8)
      tc.advance(1.second)
      h.snapshot
      assert(details.counts == expectedCounts)

      h.add(9)
      tc.advance(1.second)
      h.snapshot
      assert(details.counts == expectedCounts)

      h.add(10)
      tc.advance(1.minute - 4.seconds)
      h.snapshot()
      assert(
        details.counts == Seq(
          BucketAndCount(6, 7, 1),
          BucketAndCount(7, 8, 1),
          BucketAndCount(8, 9, 1),
          BucketAndCount(9, 10, 1),
          BucketAndCount(10, 11, 1)
        )
      )
    }
  }

  test("histogram snapshot erases old data on refresh") {
    Time.withTimeAt(BeginningOfMinute) { tc =>
      val ctx = new Ctx(tc)
      import ctx._

      // add some data (A) to the 1st window and roll
      h.add(1)
      roll()

      // initial user access to histogram snapshots
      val init = details.counts

      assert(init == Seq(BucketAndCount(1, 2, 1)))
      roll()

      // add some data (B) to the 2nd window
      Seq(-1L, 1L).foreach(h.add)
      roll()
      val countsSnap0 = details.counts
      assert(countsSnap0 == Seq(BucketAndCount(0, 1, 1), BucketAndCount(1, 2, 1)))

      // Roll to the next window, histogram should get cleared
      roll()
      val countsSnap1 = details.counts
      assert(countsSnap1 == Seq.empty)
    }
  }

  test("histogram snapshot respects latchPeriod on the first call") {
    val latchPeriod = 10.seconds
    Time.withTimeAt(BeginningOfMinute) { tc =>
      val ctx = new Ctx(tc, latchPeriod)
      import ctx._

      h.add(1)

      val snap0 = h.snapshot()
      assert(snap0.sum == 0, "snapshot is empty")

      roll(latchPeriod / 2)
      val snap1 = h.snapshot()
      assert(snap1.sum == 0, "still within window")

      roll(latchPeriod / 2)
      val snap2 = h.snapshot()
      assert(snap2.sum == 1, "snapshot is not empty")
    }
  }

}
