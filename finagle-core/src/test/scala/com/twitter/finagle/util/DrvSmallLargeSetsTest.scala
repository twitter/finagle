package com.twitter.finagle.util

import org.scalacheck.Gen
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class DrvSmallLargeSetsTest extends AnyFunSuite with ScalaCheckDrivenPropertyChecks {

  test("blows up if adding too many elements") {
    val q = new DrvSmallLargeSets(1)
    q.smallPush(1)

    intercept[IllegalStateException] {
      q.smallPush(1)
    }

    intercept[IllegalStateException] {
      q.largePush(1)
    }
  }

  test("blows up if trying to remove a non-existing element") {
    val q = new DrvSmallLargeSets(1)
    intercept[IllegalStateException] {
      q.smallPop()
    }

    intercept[IllegalStateException] {
      q.largePop()
    }
  }

  test("can add all small elements") {
    forAll(Gen.chooseNum(1, 10000)) { setSize =>
      val queue = new DrvSmallLargeSets(setSize)

      for (i <- 0 until setSize) {
        queue.smallPush(i)
        assert(queue.largeIsEmpty)
      }

      for (i <- (0 until setSize).reverse) {
        assert(!queue.smallIsEmpty)
        assert(i == queue.smallPop())
        assert(queue.largeIsEmpty)
      }

      assert(queue.smallIsEmpty)
    }
  }

  test("can add all large elements") {
    forAll(Gen.chooseNum(1, 10000)) { setSize =>
      val queue = new DrvSmallLargeSets(setSize)

      for (i <- 0 until setSize) {
        queue.largePush(i)
        assert(queue.smallIsEmpty)
      }

      for (i <- (0 until setSize).reverse) {
        assert(!queue.largeIsEmpty)
        assert(i == queue.largePop())
        assert(queue.smallIsEmpty)
      }

      assert(queue.largeIsEmpty)
    }
  }

  test("deterministic mix of small and large elements") {
    checkSeq(0 until 1000)
  }

  test("mix of small and large elements") {
    forAll(Gen.nonEmptyListOf(Gen.chooseNum(1, 10000))) { elems =>
      checkSeq(elems)
    }
  }

  private[this] def checkSeq(elems: Seq[Int]): Unit = {
    val queue = new DrvSmallLargeSets(elems.size)

    elems.foreach { i =>
      if (i % 2 == 0) queue.largePush(i)
      else queue.smallPush(i)
    }

    // Now extract them and make sure they're all still there
    elems.reverse.foreach { i =>
      if (i % 2 == 0) assert(queue.largePop() == i)
      else assert(queue.smallPop() == i)
    }

    assert(queue.largeIsEmpty)
    assert(queue.smallIsEmpty)
  }
}
