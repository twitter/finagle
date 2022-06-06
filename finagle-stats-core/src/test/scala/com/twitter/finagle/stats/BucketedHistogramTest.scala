package com.twitter.finagle.stats

import org.scalacheck.Gen
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class BucketedHistogramTest extends AnyFunSuite with ScalaCheckDrivenPropertyChecks with Matchers {
  test("percentile when empty") {
    val h = BucketedHistogram()
    assert(h.percentile(0.0) == 0)
    assert(h.percentile(0.5) == 0)
    assert(h.percentile(1.0) == 0)
  }

  private def assertWithinError(ideal: Long, actual: Long): Unit = {
    val epsilon = Math.round(ideal * BucketedHistogram.DefaultErrorPercent)
    if (epsilon == 0) {
      assert(actual == ideal)
    } else {
      actual should be(ideal +- epsilon)
    }
  }

  test("percentile 1 to 100000") {
    val h = BucketedHistogram()
    val wantedPs = Array[Double](0.0, 0.5, 0.9, 0.99, 0.999, 0.9999, 1.0)

    def assertPercentiles(maxVal: Long): Unit = {
      val actuals = h.getQuantiles(wantedPs)
      actuals.zip(wantedPs).foreach {
        case (actual, wantedP) =>
          withClue(s"percentile=$wantedP") {
            // verify that each percentile is within the error bounds.
            val ideal = Math.round(wantedP * maxVal)
            assertWithinError(ideal, actual)

            // verify that getting each percentile 1-at-a-time
            // is the same as the bulk call
            assert(h.percentile(wantedP) == actual)
          }
      }
    }

    1L.to(100L).foreach(h.add)
    assertPercentiles(100)

    101L.to(1000L).foreach(h.add)
    assertPercentiles(1000)

    1001L.to(10000L).foreach(h.add)
    assertPercentiles(10000)

    10001L.to(100000L).foreach(h.add)
    assertPercentiles(100000)
  }

  test("percentile edge cases") {
    val max = BucketedHistogram()
    max.add(Long.MaxValue)
    assert(max.percentile(0.1) == 0)
    assert(max.percentile(1.0) == Int.MaxValue)

    val zero = BucketedHistogram()
    zero.add(0)
    assert(zero.percentile(0.0) == 0)
    assert(zero.percentile(0.1) == 0)
    assert(zero.percentile(1.0) == 0)
  }

  test("clear") {
    val h = BucketedHistogram()
    assert(h.percentile(0.0) == 0)
    assert(h.percentile(0.5) == 0)
    assert(h.percentile(1.0) == 0)

    h.add(100)
    assert(h.percentile(0.0) == 0)
    assert(h.percentile(0.5) == 100)
    assert(h.percentile(1.0) == 100)

    h.clear()
    assert(h.percentile(0.0) == 0)
    assert(h.percentile(0.5) == 0)
    assert(h.percentile(1.0) == 0)
  }

  test("sum") {
    val h = BucketedHistogram()
    assert(h.sum == 0)

    h.add(100)
    h.add(200)
    assert(h.sum == 300)

    h.clear()
    assert(h.sum == 0)
  }

  test("count") {
    val h = BucketedHistogram()
    assert(h.count == 0)

    h.add(100)
    h.add(200)
    assert(h.count == 2)

    h.clear()
    assert(h.count == 0)
  }

  test("average") {
    val h = BucketedHistogram()
    assert(h.average == 0.0)

    h.add(100)
    h.add(200)
    assert(h.average == 150.0)

    h.clear()
    assert(h.average == 0)
  }

  test("outliers are handled") {
    val h = BucketedHistogram()
    h.add(2137204091L)
    h.add(-1)
    h.add(Long.MinValue)
    h.add(Long.MaxValue)
  }

  test("exporting counts starts empty") {
    val h = BucketedHistogram()
    assert(h.bucketAndCounts == Seq.empty)
  }

  test("exporting counts tracks negative and extreme values") {
    val h = BucketedHistogram()
    h.add(-1)
    h.add(0)
    h.add(1)
    h.add(1)
    h.add(1)
    h.add(Int.MaxValue)
    h.add(Int.MaxValue)
    assert(
      h.bucketAndCounts == Seq(
        BucketAndCount(0, 1, 2),
        BucketAndCount(1, 2, 3),
        BucketAndCount(2137204091, Int.MaxValue, 2)
      )
    )
  }

  test("negative values are treated as 0s") {
    val h = BucketedHistogram()
    h.add(-10)
    h.add(10)
    h.add(20)
    assert(h.minimum == 0)
    assert(h.maximum == 20)
    assert(h.count == 3)
    assert(h.average == 10)
    assert(h.sum == 30)
  }

  test("exporting counts responds to clear") {
    val h = BucketedHistogram()
    h.add(-1)
    h.add(0)
    h.add(1)
    h.add(5)
    h.add(Int.MaxValue)
    h.clear()
    assert(h.bucketAndCounts == Seq.empty)
  }

  test("percentile and min and max stays within error bounds") {
    forAll(BucketedHistogramTest.generator) {
      case (samples: List[Int], p: Double) =>
        // although this uses Gen.nonEmptyContainerOf I observed an empty List
        // generated. As an example, this failed with an NPE:
        //
        //      Occurred when passed generated values (
        //        arg0 = (List(),0.941512699565841) // 4 shrinks
        //
        // Also, observed negative values even with Gen.chooseNum constraint:
        //
        //      Occurred when passed generated values (
        //        arg0 = (List(-1),0.9370612091967268) // 33 shrinks
        //
        whenever(samples.nonEmpty && samples.forall(_ >= 0)) {
          val h = BucketedHistogram()
          samples.foreach { s => h.add(s.toLong) }

          val sorted = samples.sorted.toIndexedSeq
          val index = (Math.round(sorted.size * p).toInt - 1).max(0)
          val ideal = sorted(index).toLong
          val actual = h.percentile(p)
          assertWithinError(ideal, actual)

          // check min and max too
          assertWithinError(sorted.head, h.minimum)
          assertWithinError(sorted.last, h.maximum)
        }
    }
  }

  test("findBucket should behave the same way as binary search") {
    // scalacheck doesn't seem to respect Gen.choose limits
    val errors = (1 until 1000 by 5).toVector.map(_.toDouble / 10000.0)
    errors.foreach { error =>
      val limits = BucketedHistogram.makeLimitsFor(error)
      val h = new BucketedHistogram(error)
      forAll(Gen.choose(0, Int.MaxValue)) { num =>
        val actual = h.findBucket(num)
        val expected = Math.abs(java.util.Arrays.binarySearch(limits, num) + 1)
        assert(
          actual == expected,
          s"we saw a misalignment between the approaches for num=$num and error=$error"
        )
      }
    }
  }
}

private object BucketedHistogramTest {

  def generator =
    for {
      samples <- Gen.nonEmptyContainerOf[List, Int](Gen.chooseNum(0, Int.MaxValue))
      percentile <- Gen.choose(0.5, 0.9999)
    } yield (samples, percentile)

}
