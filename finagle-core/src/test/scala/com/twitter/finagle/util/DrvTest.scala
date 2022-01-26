package com.twitter.finagle.util

import org.scalacheck.Gen
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class DrvTest extends AnyFunSuite with ScalaCheckDrivenPropertyChecks {
  val N = 100000

  test("Drv.newVose(weights)") {
    val rng = Rng(87654321L)
    val weights = Seq.range(1, 11) map (_.toDouble)
    val drv = Drv.fromWeights(weights)
    val histo = new Array[Int](10)
    for (_ <- 0 until N)
      histo(drv(rng)) += 1

    for (i <- 1 to 9) {
      val a = histo(i - 1) / i
      val b = histo(i) / (i + 1)
      assert(math.abs(a - b).toDouble / histo(i) < 0.005)
    }
  }

  test("Drv.newVose(equal distribution)") {
    val Drv.Aliased(_, prob) = Drv.newVose(Array.fill(10) { 0.1 })
    assert(prob forall (_ == 1.0))
  }

  test("Drv.newVose(zero probs)") {
    val Drv.Aliased(aliased, prob) = Drv.newVose(Array(1, 0))
    assert(aliased(1) == 0)
    assert(prob(0) == 1.0)
    assert(prob(1) == 0.0)
  }

  test("Drv.newVose(all zeros)") {
    val Drv.Aliased(_, prob) = Drv.newVose(Array(0, 0, 0))
    assert(prob forall (_ == 1))
  }

  test("Drv.fromWeights(nil)") {
    intercept[IllegalArgumentException] { Drv.fromWeights(Nil) }
  }

  test("Drv.apply(nil)") {
    intercept[IllegalArgumentException] { Drv(Nil) }
  }

  private[this] def normalize(numbers: Seq[Double]): IndexedSeq[Double] = {
    val sum = numbers.sum
    if (sum == 0d) {
      // Must all be zeros
      val len = numbers.length
      Vector.fill(len) {
        1d / len
      }
    } else {
      numbers.iterator.map(_ / sum).toIndexedSeq
    }
  }

  private[this] def assertAliasTableIsCorrect(probabilities: IndexedSeq[Double]): Unit = {
    val Drv.Aliased(aliased, prob) = Drv.fromWeights(probabilities)

    // We're going to reverse engineer the probability distribution from
    // the alias tables. This is pretty straight forward: for every index `i`
    // we simply ascribe the probability of that index to the index itself
    // weighted by it's probability and scribe the rest of the probability
    // to the element in it's alias table.
    val p = new Array[Double](probabilities.length)
    p.indices.foreach { i =>
      p(i) += prob(i)
      p(aliased(i)) += (1d - prob(i))
    }

    // At the end we need to normalize since all our probabilities were scaled
    // to 1.0 in order to play nicely with the random number generator which
    // happens to generate floating point numbers on the range [0.0, 1.0).
    p.indices.foreach { i =>
      p(i) /= p.length
    }

    // now make sure they match
    p.indices.foreach { i =>
      val epsilon = 0.00001
      assert(math.abs(probabilities(i) - p(i)) < epsilon)
    }
  }

  test("Reverse engineer simple distributions") {
    val distributions: Seq[IndexedSeq[Double]] = Seq(
      Seq(0d),
      Seq(1d),
      Seq(0d, 1d),
      Seq(0d, 0d, 1d),
      Seq(1d, 1d, 1d),
      Seq(1d, 1d, 2d),
    ).map(normalize(_))

    distributions.foreach { dist =>
      assertAliasTableIsCorrect(dist)
    }
  }

  test("Property test that Drv alias tables can be reverse engineered") {
    val normalizedNonEmptyList: Gen[IndexedSeq[Double]] =
      Gen.nonEmptyListOf(Gen.chooseNum(0d, 1e5)).map(normalize(_))

    forAll(normalizedNonEmptyList) { probabilities =>
      assertAliasTableIsCorrect(probabilities)
    }
  }
}
