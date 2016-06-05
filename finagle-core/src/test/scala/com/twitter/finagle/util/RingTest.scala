package com.twitter.finagle.util

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RingTest extends FunSuite {
  val N = 100000
  val W = Int.MaxValue

  def newRng() = Rng(33334L)

  def assertBalanced(numSlices: Int, histo: Map[Int, Int]) {
    for (Seq(i, j) <- (0 until numSlices).sliding(2)) {
      val a = histo(i).toDouble
      val b = histo(j).toDouble

      assert(math.abs(a-b)/math.max(a,b) < 0.15,
        s"$i=$a,$j=$b is unbalanced. "+
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

    val numSlices = 6
    val r = Ring(numSlices, W)

    for (off <- Seq(0, W/2, W)) {
      val histo = run { Seq(r.pick(rng, off, W)) }
      assertBalanced(numSlices, histo)
    }
  }

  test("pick2: always pick different indices when available; even") {
    val rng = newRng()
    val numSlices = 4
    val r = Ring(numSlices, W)

    val histo = run {
      val (a, b) = r.pick2(rng, 0, W)
      assert(a != b)
      Seq(a, b)
    }

    assertBalanced(numSlices, histo)
  }

  test("pick2: partial, overflow") {
    val rng = newRng()
    val numSlices = 3
    val r = Ring(numSlices, W)
    var count = 0
    val histo = run {
      val (a, b) = r.pick2(rng, (3L*W/4L).toInt, W/2)
      if (a == b) count += 1
      Seq(a, b)
    }
    assert(histo.keySet == Set(0, 1, 2))
    assert(count == 0)
  }
}