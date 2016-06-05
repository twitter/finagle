package com.twitter.finagle.util

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DrvTest extends FunSuite {
  val N = 100000

  test("Drv.newVose(weights)") {
    val rng = Rng(87654321L)
    val weights = Seq.range(1, 11) map (_.toDouble)
    val drv = Drv.fromWeights(weights)
    val histo = new Array[Int](10)
    for (_ <- 0 until N)
      histo(drv(rng)) += 1

    for (i <- 1 to 9) {
      val a = histo(i-1)/i
      val b = histo(i)/(i+1)
      assert(math.abs(a-b).toDouble/histo(i) < 0.005)
    }
  }

  test("Drv.newVose(equal distribution)") {
    val Drv.Aliased(aliased, prob) = Drv.newVose(Seq.fill(10) { 0.1 })
    assert(prob forall (_==1.0))
  }

  test("Drv.newVose(zero probs)") {
    val Drv.Aliased(aliased, prob) = Drv.newVose(Seq(1, 0))
    assert(aliased(1) == 0)
    assert(prob(0) == 1.0)
    assert(prob(1) == 0.0)
  }

  test("Drv.newVose(all zeros)") {
    val Drv.Aliased(_, prob) = Drv.newVose(Seq(0, 0, 0))
    assert(prob forall(_ == 1))
  }
}
