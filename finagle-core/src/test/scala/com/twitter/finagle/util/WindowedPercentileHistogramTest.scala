package com.twitter.finagle.util

import com.twitter.conversions.PercentOps._
import com.twitter.conversions.DurationOps._
import com.twitter.finagle.stats.BucketAndCount
import com.twitter.util.{MockTimer, Time}
import org.scalacheck.Gen
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import scala.collection.mutable
import org.scalatest.funsuite.AnyFunSuite

class WindowedPercentileHistogramTest
    extends AnyFunSuite
    with Eventually
    with IntegrationPatience
    with ScalaCheckDrivenPropertyChecks {

  test("Throws IllegalArgumentException when retrieving a percentile < 0") {
    val wp = new WindowedPercentileHistogram(10, 10.seconds, new MockTimer)
    wp.percentile(0)
    intercept[IllegalArgumentException] {
      wp.percentile(-0.5.percent)
    }
  }

  test("Throws IllegalArgumentException when retrieving a percentile > 100") {
    val wp = new WindowedPercentileHistogram(10, 10.seconds, new MockTimer)
    wp.percentile(100.percent)
    intercept[IllegalArgumentException] {
      wp.percentile(100.5.percent)
    }
  }

  test("Percentile is initially 0") {
    val wp = new WindowedPercentileHistogram(10, 10.seconds, new MockTimer)
    assert(wp.percentile(50.percent) == 0)
    wp.close()
  }

  test("Gets fractional percentiles to two decimal places accurately") {
    val timer = new MockTimer
    0.until(9999).foreach { i: Int =>
      Time.withCurrentTimeFrozen { tc =>
        val wp = new WindowedPercentileHistogram(10, 10.seconds, timer)
        val percentile = i.toDouble / 100
        val initial = (10000 - i - 1)
        0.until(initial).foreach(_ => wp.add(10))
        tc.advance(10.seconds)
        timer.tick()
        assert(wp.percentile(percentile.percent) == 10)
        0.until(10000 - initial).foreach(_ => wp.add(5))
        tc.advance(10.seconds)
        timer.tick()
        assert(wp.percentile(percentile.percent) == 5)
        wp.close()
      }
    }
  }

  test("Buckets are flushed each `bucketSize`") {
    val timer = new MockTimer
    Time.withCurrentTimeFrozen { tc =>
      val wp = new WindowedPercentileHistogram(10, 10.seconds, timer)
      wp.add(100)
      assert(wp.percentile(0.0) == 0)
      tc.advance(10.seconds)
      timer.tick()
      eventually {
        assert(wp.percentile(0.0) == 100)
        assert(wp.percentile(50.percent) == 100)
        assert(wp.percentile(100.percent) == 100)
      }
      wp.close()
    }
  }

  test("Sliding window") {
    val timer = new MockTimer
    Time.withCurrentTimeFrozen { tc =>
      val wp = new WindowedPercentileHistogram(3, 10.seconds, timer)

      wp.add(1)
      wp.add(2)
      wp.add(3)

      tc.advance(10.seconds)
      timer.tick()

      eventually {
        assert(wp.percentile(50.percent) == 2) // median of (1, 2, 3)
      }

      wp.add(4)
      wp.add(5)
      wp.add(6)
      wp.add(7)

      tc.advance(10.seconds)
      timer.tick()

      eventually {
        assert(wp.percentile(50.percent) == 4) // median of (1, 2, 3) ∪ (4, 5, 6, 7)
      }

      wp.add(8)
      wp.add(9)
      wp.add(10)

      tc.advance(10.seconds)
      timer.tick() // flush adds

      tc.advance(10.seconds) // first bucket (1, 2, 3) falls out of window
      timer.tick()

      eventually {
        assert(wp.percentile(0.0) == 4) // min of (4, 5, 6, 7) ∪ (8, 9, 10)
        assert(wp.percentile(50.percent) == 7) // median of (4, 5, 6, 7) ∪ (8, 9, 10)
        assert(wp.percentile(100.percent) == 10) // max of (4, 5, 6, 7) ∪ (8, 9, 10)
      }
      wp.close()
    }
  }

  test("Caps values at DefaultHighestTrackableValue") {
    val timer = new MockTimer
    Time.withCurrentTimeFrozen { tc =>
      val highestTrackable = WindowedPercentileHistogram.DefaultHighestTrackableValue + 10
      val wp = new WindowedPercentileHistogram(
        3,
        10.seconds,
        WindowedPercentileHistogram.DefaultLowestDiscernibleValue,
        highestTrackable,
        timer
      )

      wp.add(highestTrackable + 10)
      tc.advance(10.seconds)
      timer.tick()
      eventually {
        assert(wp.percentile(50.percent) == highestTrackable)
      }
    }
  }

  test("Has precision of +/- 1 at 1000") {
    val timer = new MockTimer
    1.until(WindowedPercentileHistogram.DefaultHighestTrackableValue / 1000).foreach { i =>
      Time.withCurrentTimeFrozen { tc =>
        val wp = new WindowedPercentileHistogram(3, 10.seconds, timer)

        wp.add(1000 * i)
        assert(wp.percentile(0.0) == 0)
        tc.advance(10.seconds)
        timer.tick()
        eventually {
          assert(
            wp.percentile(50.percent) >= ((1000 * i) - i) &&
              wp.percentile(50.percent) <= ((1000 * i) + i)
          )
        }
      }
    }
  }

  test("Can convert to BucketAndCount") {
    val timer = new MockTimer
    Time.withCurrentTimeFrozen { tc =>
      val wp = new WindowedPercentileHistogram(3, 10.seconds, timer)

      (0 to 3).foreach(i => wp.add(i))

      tc.advance(10.seconds)
      timer.tick()

      eventually {
        assert(
          wp.toBucketAndCounts().toSet == generateBucketAndCounts(Seq(0, 1, 2, 3), 2000, wp).toSet
        )
      }

      wp.close()
    }
  }

  test("Non-contiguous conversion to BucketAndCounts") {
    val timer = new MockTimer
    Time.withCurrentTimeFrozen { tc =>
      val wp = new WindowedPercentileHistogram(3, 10.seconds, timer)

      wp.add(1)
      wp.add(50)
      wp.add(300)

      tc.advance(10.seconds)
      timer.tick()

      eventually {
        assert(
          wp.toBucketAndCounts().toSet == generateBucketAndCounts(Seq(1, 50, 300), 2000, wp).toSet
        )
      }

      wp.close()
    }
  }

  test("Sliding window with BucketAndCount conversion") {
    val timer = new MockTimer
    Time.withCurrentTimeFrozen { tc =>
      val wp = new WindowedPercentileHistogram(3, 10.seconds, timer)

      wp.add(1)
      wp.add(2)
      wp.add(3)

      tc.advance(10.seconds)
      timer.tick()

      eventually {
        assert(
          wp.toBucketAndCounts().toSet == generateBucketAndCounts(Seq(1, 2, 3), 2000, wp).toSet
        )
      }

      wp.add(3)
      wp.add(4)
      wp.add(5)

      tc.advance(10.seconds)
      timer.tick()

      eventually {
        assert(
          wp.toBucketAndCounts().toSet == generateBucketAndCounts(
            Seq(1, 2, 3, 3, 4, 5),
            2000,
            wp).toSet
        )
      }

      wp.add(4)
      wp.add(5)
      wp.add(7)

      tc.advance(10.seconds)
      timer.tick()

      tc.advance(10.seconds) // first bucket falls out of the window
      timer.tick()

      eventually {
        assert(
          wp.toBucketAndCounts().toSet == generateBucketAndCounts(
            Seq(3, 4, 4, 5, 5, 7),
            2000,
            wp).toSet
        )
      }

      wp.close()
    }
  }

  test("BucketAndCounts of histogram with really high highest trackable value") {
    val timer = new MockTimer
    Time.withCurrentTimeFrozen { tc =>
      val wp = new WindowedPercentileHistogram(3, 10.seconds, 1, 10000, timer)

      wp.add(3)
      wp.add(2005)
      wp.add(2048)
      wp.add(3998)
      wp.add(5000)
      wp.add(9999)
      wp.add(10000)

      tc.advance(10.seconds)
      timer.tick()

      eventually {
        assert(
          wp.toBucketAndCounts().toSet == generateBucketAndCounts(
            Seq(3, 2005, 2048, 3998, 5000, 9999, 10000),
            10000,
            wp).toSet
        )
      }

      wp.close()
    }
  }

  test("BucketAndCounts of histogram with fuzz") {
    val histogramInputs = Gen.listOfN(10, Gen.chooseNum(1, 10000))
    var listOfInputsToCreateBucketAndCounts = mutable.ArrayBuffer[Int]()

    val timer = new MockTimer
    Time.withCurrentTimeFrozen { tc =>
      val wp = new WindowedPercentileHistogram(3, 10.seconds, 1, 10000, timer)

      forAll(histogramInputs) { i =>
        i.foreach(n => {
          wp.add(n)
          listOfInputsToCreateBucketAndCounts += n
        })
      }

      tc.advance(10.seconds)
      timer.tick()

      eventually {
        assert(
          wp.toBucketAndCounts().toSet == generateBucketAndCounts(
            listOfInputsToCreateBucketAndCounts.toSeq,
            10000,
            wp).toSet)
      }

      wp.close()
    }
  }

  /**
   * Generates a sequence of BucketAndCounts given a list of inputs.
   *
   * Assumes significantDigits is 3, because it is defined as such in WindowedPercentileHistogram.
   * See the HdrPrecision value.
   */
  def generateBucketAndCounts(
    input: Seq[Int],
    histogramHighestTrackableValue: Int,
    wp: WindowedPercentileHistogram,
    significantDigits: Int = 3
  ): Seq[BucketAndCount] = {

    def getBucketForValue(value: Int, count: Int): Option[BucketAndCount] = {
      if (value <= histogramHighestTrackableValue)
        Some(wp.calculateBucketForValue(value, count, significantDigits))
      else None
    }

    def mergeBuckets(bucketAndCounts: Seq[BucketAndCount]): Seq[BucketAndCount] = {
      bucketAndCounts
        .groupBy(_.lowerLimit).map {
          case (_, bucketAndCountList) =>
            BucketAndCount(
              bucketAndCountList.head.lowerLimit,
              bucketAndCountList.head.upperLimit,
              bucketAndCountList.foldLeft(0) { case (acc, b) => acc + b.count })
        }.toSeq
    }

    mergeBuckets(
      input
        .groupBy(i => i).flatMap {
          case (value, countList) => getBucketForValue(value, countList.size)
        }.toSeq)
  }
}
