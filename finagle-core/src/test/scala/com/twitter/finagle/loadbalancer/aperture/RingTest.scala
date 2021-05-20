package com.twitter.finagle.loadbalancer.aperture

import com.twitter.finagle.util.Rng
import org.scalacheck.Gen
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.funsuite.AnyFunSuite

class RingTest extends AnyFunSuite with ScalaCheckDrivenPropertyChecks {
  val rng = Rng(12345L)

  def histo(seq: Seq[Int]): Map[Int, Int] =
    seq.foldLeft(Map.empty[Int, Int]) {
      case (map, i) => map + (i -> (map.getOrElse(i, 0) + 1))
    }

  val numRuns = 100000
  def run(mk: => Seq[Int]): Map[Int, Int] = histo(Seq.fill(numRuns)(mk).flatten)

  def assertBalanced(histo: Map[Int, Int]): Unit = {
    for (Seq(i, j) <- histo.keys.sliding(2)) {
      val a = histo(i).toDouble
      val b = histo(j).toDouble

      withClue(s"$i=$a, $j=$b is unbalanced: histo=$histo") {
        assert(math.abs(a - b) / math.max(a, b) < 0.1)
      }
    }
  }

  def assertApproxEqual(a: Double, b: Double, epsilon: Double = 1e-4): Unit = {
    withClue(s"${a} was not approximately equal to ${b}") {
      assert(Math.abs(a - b) < epsilon)
    }
  }

  test("Ring.range has a valid size") {
    val numClients = 65
    val localUnitWidth = 1.0 / numClients
    val minAperture = 12
    val numRemote = 20

    forAll(Gen.choose(1, numRemote), Gen.choose(0, numClients)) { (remoteSize, clientId) =>
      // For whatever reason the generator doesn't respect the lower bound
      whenever(remoteSize > 0) {
        val remoteUnitWidth = 1.0 / remoteSize
        val offset: Double = (clientId * localUnitWidth) % 1.0

        val ring = new Ring(remoteSize, Rng(123))
        val apWidth =
          DeterministicAperture.dApertureWidth(localUnitWidth, remoteUnitWidth, minAperture)

        withClue(s"clientId: $clientId, offset: $offset, aperture width: $apWidth") {
          val ringRange = ring.range(offset, apWidth)
          assert(ring.indices(offset, apWidth).size == ringRange)
          assert(ringRange >= math.min(remoteSize, minAperture))
          assert(ringRange <= remoteSize)
        }
      }
    }
  }

  test("Ring.range has a valid size with tricky values") {
    // These were values found to fail as part of CSL-9607.
    val offset = 0.09230769230769231 // clientId = 6 of 65
    val apertureWidth = 0.9230769230769231
    val remoteSize = 13
    val minAperture = 12

    val ring = new Ring(remoteSize, Rng(123))
    val ringRange = ring.range(offset, apertureWidth)

    assert(ring.indices(offset, apertureWidth).size == ringRange)
    assert(ringRange >= math.min(remoteSize, minAperture))
    assert(ringRange <= remoteSize)
  }

  test("pick2") {
    val ring = new Ring(10, rng)
    val offset = 3 / 4d
    val width = 1 / 2d

    val histo0 = run {
      val (a, b) = ring.pick2(offset, width)
      if (a == b) {
        fail(s"pick2: (a=$a, b=$b)")
      }
      Seq(a, b)
    }

    val histo1 = run {
      val a = ring.pick(offset, width)
      var b = a
      while (a == b) {
        b = ring.pick(offset, width)
      }
      Seq(a, b)
    }

    for (key <- histo0.keys) {
      val a = histo0(key)
      val b = histo1(key)
      withClue(s"pick2 is biased towards $key: pick2=$histo0 pick=$histo1") {
        assert(math.abs(a - b) / math.max(a, b) < 1e-3)
      }
    }
  }

  test("pick2: no range") {
    val ring = new Ring(10, rng)
    val (a, b) = ring.pick2(3 / 4d, 0d)
    assert(a == 7)
    assert(a == b)

    for (_ <- 0 to 100) {
      val (a, b) = ring.pick2(rng.nextDouble, 0d)
      assert(a == b)
    }
  }

  test("pick2: full range") {
    Seq(10, 1250, 2000, 4422, 7200, 10000).foreach { size =>
      val ring = new Ring(size, rng)
      val histo = run {
        val (a, b) = ring.pick2(0.0, 1.0)
        Seq(a, b)
      }
      assert(histo.size == size)
    }
  }

  test("range boundary cases") {
    val r0 = new Ring(1, rng)
    assert(r0.range(rng.nextDouble, 0.0) == 1)
    assert(r0.range(rng.nextDouble, 0.5) == 1)
    assert(r0.range(rng.nextDouble, 1.0) == 1)

    val size = 2 + (rng.nextDouble * 10000).toInt
    val r1 = new Ring(size, rng)
    assert(r1.range(rng.nextDouble, 0.0) == 1)
    assert(r1.range(rng.nextDouble, Double.MinPositiveValue) == 1)
    assert(r1.range(rng.nextDouble, 0.000001) == 1)
    assert(r1.range(rng.nextDouble, .999999) == size)
    assert(r1.range(rng.nextDouble, 1.0 - Double.MinPositiveValue) == size)
    assert(r1.range(rng.nextDouble, 1.0) == size)

    forAll(Gen.posNum[Int], Gen.posNum[Int]) { (size, id) =>
      whenever(size > 0 && id > 0) {
        val r = new Ring(size, rng)
        assert(r.range(id.toDouble / size, 1.0) == size)
      }
    }
  }

  test("range projects pick2") {
    val ring = new Ring(10, rng)
    def histo(offset: Double, width: Double) = run {
      val (a, b) = ring.pick2(offset, width)
      Seq(a, b)
    }

    // aligned with ring units
    val h0 = histo(offset = .1d, width = .3d)
    val r0 = ring.range(offset = .1d, width = .3d)
    assert(h0.keys == Set(1, 2, 3))
    assert(r0 == h0.keys.size)

    // misaligned at end
    val h1 = histo(offset = .1d, width = .35d)
    val r1 = ring.range(offset = .1d, width = .35d)
    assert(h1.keys == Set(1, 2, 3, 4))
    assert(r1 == h1.keys.size)

    // misaligned at start
    val h2 = histo(offset = .35d, width = .15d)
    val r2 = ring.range(offset = .35d, width = .15d)
    assert(h2.keys == Set(3, 4))
    assert(r2 == h2.keys.size)

    // misaligned at both
    val h3 = histo(offset = .25d, width = .2d)
    val r3 = ring.range(offset = .25d, width = .2d)
    assert(h3.keys == Set(2, 3, 4))
    assert(r3 == h3.keys.size)
  }

  test("pick2: wraps around and respects weights") {
    val ring = new Ring(10, rng)
    val histo = run {
      val (a, b) = ring.pick2(3 / 4d, 1 / 2d)
      Seq(a, b)
    }

    assert(ring.range(3 / 4d, 1 / 2d) == histo.keys.size)
    assert(histo.keys == Set(7, 8, 9, 0, 1, 2))
    // 7 and 2 get a fraction of the traffic
    val f: ((Int, Int)) => Boolean = { kv => kv._1 == 2 || kv._1 == 7 }
    assertBalanced(histo.filter(f))
    assertBalanced(histo.filterNot(f))
    // We know that index 2 and 7 should get ~50% of picks relative
    // to the other indices.
    val a = histo(1).toDouble
    val b = histo(2).toDouble
    assert(b / a - 0.5 < 1e-1)
  }

  test("Handles full-width windows when offset + width results in an overlap") {
    // This bug case was reported externally:
    // https://nvartolomei.com/weighted-deterministic-aperture/
    // https://twitter.com/nvartolomei/status/1295010014457987073
    val ring = new Ring(4, rng)
    val weight = ring.weight(0, 1 / 8D, 1D)
    assertApproxEqual(weight, 1.0D)
  }

  test("weight") {
    val ring = new Ring(10, rng)

    assert(ring.range(1 / 4d, 1 / 4d) == 3)
    assert(ring.indices(1 / 4d, 1 / 4d) == Seq(2, 3, 4))

    // wrap around
    assert(ring.range(3 / 4d, 1 / 2d) == 6)
    assert(ring.indices(3 / 4d, 1 / 2d) == Seq(7, 8, 9, 0, 1, 2))

    // walk the indices
    for (i <- 0 to 9) {
      withClue(s"offset=${i / 10d} width=${1 / 10d}") {
        assert(ring.range(i / 10d, 1 / 10d) == 1)
        assert(ring.indices(i / 10d, 1 / 10d) == Seq(i % 10))
      }
    }

    assertApproxEqual(1.0, ring.weight(index = 5, offset = 0.5D, 0.1D))
    assertApproxEqual(1.0, ring.weight(index = 5, offset = 0.5D, 1.0D))

    // Wrap-around: intersection between [0.0, 0.1) and [0.75, 1.25)
    assertApproxEqual(1.0, ring.weight(index = 0, offset = 0.75D, 0.5D))

    assertApproxEqual(ring.weight(index = 0, offset = 0, width = 1.0), 1.0)

    // Wrap-around with full width, zero offset, and nonzero index
    assertApproxEqual(ring.weight(index = 9, offset = 0, width = 1.0), 1.0)
    // Wrap-around with full width, non-zero offset, and nonzero index
    assertApproxEqual(ring.weight(index = 9, offset = 0.5D, width = 1.0), 1.0)
    // Wrap-around with full width, zero offset, and zero index
    assertApproxEqual(ring.weight(index = 0, offset = 0, width = 1.0), 1.0)

    //Full overlap without offset
    assertApproxEqual(ring.weight(index = 0, offset = 0, width = 0.1D), 1.0D)
    // Full overlap with offset
    assertApproxEqual(ring.weight(index = 1, offset = 0.1D, width = 0.1D), 1.0D)
  }

  test("Weight should be 0 if offset > unitWidth and we don't overlap the ring boundary") {
    val ring = new Ring(10, rng)
    val offsetGen: Gen[Double] = Gen.choose[Int](10, 89).map { n => n * 0.01D }

    forAll(offsetGen) { offset =>
      assertApproxEqual(ring.weight(0, offset, 0.1D), 0D)
    }
  }

  test("Partial weights without overlapping the ring boundary") {
    val ring = new Ring(10, rng)
    val offsetGen: Gen[Double] = Gen.choose(0, 99).map { n => n * 0.001D }

    forAll(offsetGen) { offset =>
      val unitWidth = 0.1D
      assert(offset < unitWidth)
      val overlapping = (unitWidth - offset) / unitWidth
      assertApproxEqual(ring.weight(0, offset, width = 0.1D), overlapping)
    }
  }

  test("Weight should be 0 if width is 0") {
    val ring = new Ring(10, rng)

    // With a width of 0, the overlap should be 0 regardless of offset
    val offsetGen: Gen[Double] = Gen.choose(0, 100).map { n => n * .01D }
    val indexGen: Gen[Int] = Gen.choose(0, 9)

    forAll(offsetGen, indexGen) { (offset, index) =>
      assertApproxEqual(ring.weight(index = index, offset = offset, width = 0.0D), 0D)
    }
  }

  test("weight throws exceptions when appropriate") {
    val ring = new Ring(10, rng)
    assertThrows[IllegalArgumentException] {
      ring.weight(10, 0D, 0.5D)
    }

    assertThrows[IllegalArgumentException] {
      ring.weight(0, 0D, 1.5D)
    }

    assertThrows[IllegalArgumentException] {
      ring.weight(0, 0D, -0.5D)
    }
  }
}
