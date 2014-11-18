package com.twitter.finagle.util

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RingTest extends FunSuite {
  val N = 100000
  val W = Int.MaxValue
  
  def newRng() = Rng(33334L)
  
  def assertBalanced(weights: Seq[Double], histo: Map[Int, Int]) {
    for (Seq(i, j) <- weights.indices.sliding(2)) {
      val aw = weights(i)
      val bw = weights(j)
      val an = histo(i)
      val bn = histo(j)

      val a = an/aw
      val b = bn/bw

      assert(math.abs(a-b)/math.max(a, b) < 0.15, 
        s"$i=$a,$j=$b is unbalanced. Weights:${weights.mkString(",")} "+
        s"Histo:${histo.mkString(",")}")
    }
  }
  
  def histo(seq: Seq[Int]) =
    seq.foldLeft(Map.empty: Map[Int, Int]) { 
      case (map, which) => map + (which -> (map.getOrElse(which, 0) + 1))
    }

  def run(mk: => Seq[Int]) =
    histo(Seq.fill(N)(mk).flatten)
    
    
  test("pick: full range") {
    val rng = newRng()

    val weightss = Seq[Double](1,1,1,1,1,1) +: Seq.fill(100) { 
      Seq.fill(6) { 0.01 + 0.99*rng.nextDouble() }
    }

    for (off <- Seq(0, W/2, W)) {
      for (weights <- weightss) {
        val r = Ring.fromWeights(weights, W)
        val histo = run { Seq(r.pick(rng, off, W)) }
        assertBalanced(weights, histo)
      }
    }
  }

  test("pick2: always pick different indices when available; even") {
    val rng = newRng()
    val weights = Seq[Double](1,1,1,1)
    val r = Ring.fromWeights(weights, W)

    val histo = run {
      val (a, b) = r.pick2(rng, 0, W)
      assert(a != b)
      Seq(a, b)
    }

    assertBalanced(weights, histo)
  }
  
  test("pick2: partial, overflow") {
    val rng = newRng()
    val weights = Seq[Double](1, 1, 2)
    val r = Ring.fromWeights(weights, W)
    var count = 0
    val histo = run {
      val (a, b) = r.pick2(rng, (3L*W/4L).toInt, W/2)
      if (a == b)
        count += 1
      //assert(a != b)
      Seq(a, b)
    }
    assert(histo.keySet === Set(2, 0))
    assert(count === 0)
  }
  
  test("overlapping positions / zero weights") {
    val rng = newRng()

    for (pos <- 0 to 2) {
      val weights = Seq.fill[Double](3)(0).updated(pos, 1.0)
      val r = Ring.fromWeights(weights, W)
      val histo = run { Seq(r.pick(rng, 0, W)) }
      assert(histo.keys === Set(pos))
    }
  }
}
