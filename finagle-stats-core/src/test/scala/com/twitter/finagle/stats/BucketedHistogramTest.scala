package com.twitter.finagle.stats

import org.scalacheck.Gen
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class BucketedHistogramTest extends AbstractBucketedHistogramTest {
  override protected def createNewHistogram(): AbstractBucketedHistogram = BucketedHistogram()
}
class LockFreeBucketedHistogramTest extends AbstractBucketedHistogramTest {
  override protected def createNewHistogram(): AbstractBucketedHistogram =
    new LockFreeBucketedHistogram(
      BucketedHistogram.DefaultErrorPercent
    )
}

abstract class AbstractBucketedHistogramTest
    extends AnyFunSuite
    with ScalaCheckDrivenPropertyChecks
    with Matchers {

  protected def createNewHistogram(): AbstractBucketedHistogram

  test("percentile when empty") {
    val h = createNewHistogram()
    val snapshot = new BucketedHistogram.MutableSnapshot(
      Array(0.0, 0.5, 1.0)
    )
    h.recompute(snapshot)
    assert(snapshot.quantiles(0) == 0)
    assert(snapshot.quantiles(1) == 0)
    assert(snapshot.quantiles(2) == 0)
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
    val h = createNewHistogram()
    val wantedPs = Array[Double](0.0, 0.5, 0.9, 0.99, 0.999, 0.9999, 1.0)
    val snapshot = new BucketedHistogram.MutableSnapshot(
      wantedPs
    )

    def assertPercentiles(maxVal: Long): Unit = {
      h.recompute(snapshot)
      val actuals = snapshot.quantiles
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
    val snapshot = new BucketedHistogram.MutableSnapshot(
      Array(0.0, 0.5, 1.0)
    )

    val max = createNewHistogram()
    max.add(Long.MaxValue)

    max.recompute(snapshot)
    assert(snapshot.quantiles(0) == 0)
    assert(snapshot.quantiles(2) == Int.MaxValue)

    val zero = createNewHistogram()
    zero.add(0)

    zero.recompute(snapshot)
    assert(snapshot.quantiles(0) == 0)
    assert(snapshot.quantiles(1) == 0)
    assert(snapshot.quantiles(2) == 0)
  }

  test("clear") {
    val snapshot = new BucketedHistogram.MutableSnapshot(
      Array(0.0, 0.5, 1.0)
    )
    val h = createNewHistogram()
    h.recompute(snapshot)
    assert(snapshot.quantiles(0) == 0)
    assert(snapshot.quantiles(1) == 0)
    assert(snapshot.quantiles(2) == 0)

    h.add(100)
    h.recompute(snapshot)
    assert(snapshot.quantiles(0) == 0)
    assert(snapshot.quantiles(1) == 100)
    assert(snapshot.quantiles(2) == 100)

    h.clear()
    h.recompute(snapshot)
    assert(snapshot.quantiles(0) == 0)
    assert(snapshot.quantiles(1) == 0)
    assert(snapshot.quantiles(2) == 0)
  }

  test("sum") {
    val snapshot = new BucketedHistogram.MutableSnapshot(
      Array(0.0, 0.5, 1.0)
    )
    val h = createNewHistogram()
    h.recompute(snapshot)
    assert(snapshot.sum == 0)

    h.add(100)
    h.add(200)
    h.recompute(snapshot)
    assert(snapshot.sum == 300)

    h.clear()
    h.recompute(snapshot)
    assert(snapshot.sum == 0)
  }

  test("count") {
    val snapshot = new BucketedHistogram.MutableSnapshot(
      Array(0.0, 0.5, 1.0)
    )
    val h = createNewHistogram()
    h.recompute(snapshot)
    assert(snapshot.count == 0)

    h.add(100)
    h.add(200)
    h.recompute(snapshot)
    assert(snapshot.count == 2)

    h.clear()
    h.recompute(snapshot)
    assert(snapshot.count == 0)
  }

  test("average") {
    val snapshot = new BucketedHistogram.MutableSnapshot(
      Array(0.0, 0.5, 1.0)
    )
    val h = createNewHistogram()
    h.recompute(snapshot)
    assert(snapshot.avg == 0.0)

    h.add(100)
    h.add(200)
    h.recompute(snapshot)
    assert(snapshot.avg == 150.0)

    h.clear()
    h.recompute(snapshot)
    assert(snapshot.avg == 0)
  }

  test("outliers are handled") {
    val h = createNewHistogram()
    h.add(2137204091L)
    h.add(-1)
    h.add(Long.MinValue)
    h.add(Long.MaxValue)
  }

  test("exporting counts starts empty") {
    val h = createNewHistogram()
    assert(h.bucketAndCounts == Seq.empty)
  }

  test("exporting counts tracks negative and extreme values") {
    val h = createNewHistogram()
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
    val snapshot = new BucketedHistogram.MutableSnapshot(
      Array(0.0, 0.5, 1.0)
    )
    val h = createNewHistogram()
    h.add(-10)
    h.add(10)
    h.add(20)
    h.recompute(snapshot)
    assert(snapshot.min == 0)
    assert(snapshot.max == 20)
    assert(snapshot.count == 3)
    assert(snapshot.avg == 10)
    assert(snapshot.sum == 30)
  }

  test("exporting counts responds to clear") {
    val h = createNewHistogram()
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
          val snapshot = new BucketedHistogram.MutableSnapshot(
            Array(p)
          )
          val h = createNewHistogram()
          samples.foreach { s => h.add(s.toLong) }
          h.recompute(snapshot)
          val sorted = samples.sorted.toIndexedSeq
          val index = (Math.round(sorted.size * p).toInt - 1).max(0)
          val ideal = sorted(index).toLong
          val actual = snapshot.quantiles(0)
          assertWithinError(ideal, actual)

          // check min and max too
          assertWithinError(sorted.head, snapshot.min)
          assertWithinError(sorted.last, snapshot.max)
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
