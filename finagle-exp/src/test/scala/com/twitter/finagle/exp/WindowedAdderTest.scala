package com.twitter.finagle.exp

import com.twitter.util.{Duration, Stopwatch}
import org.junit.runner.RunWith
import org.scalatest.{FunSuite, BeforeAndAfterEach}
import org.scalatest.junit.JUnitRunner
import com.twitter.conversions.time._

@RunWith(classOf[JUnitRunner])
class WindowedAdderTest extends FunSuite with BeforeAndAfterEach {
  val sw = new ManualStopwatch
  private val adder = new WindowedAdder(3.seconds, 3, sw)
  
  override def beforeEach() {
    adder.reset()
  }

  test("sums things up when time stands still") {
    adder.add(1)
    assert(adder.sum() === 1)
    adder.add(1)
    assert(adder.sum() === 2)
    adder.add(3)
    assert(adder.sum() === 5)
  }

  test("maintains a sliding window") {
    adder.add(1)
    assert(adder.sum() === 1)
    sw.tick(1.second)
    assert(adder.sum() === 1)
    adder.add(2)
    assert(adder.sum() === 3)
    sw.tick(1.second)
    assert(adder.sum() === 3)
    sw.tick(1.second)
    assert(adder.sum() === 2)
    sw.tick(1.second)
    assert(adder.sum() === 0)
  }
  
  test("maintains a sliding window when slices are skipped") {
    adder.incr()
    assert(adder.sum() === 1)
    sw.tick(1.seconds)
    adder.add(2)
    assert(adder.sum() === 3)
    sw.tick(1.second)
    adder.incr()
    assert(adder.sum() === 4)

    sw.tick(2.seconds)
    assert(adder.sum() === 1)

    sw.tick(100.seconds)
    assert(adder.sum() === 0)
    
    adder.add(100)
    sw.tick(1.second)
    assert(adder.sum() === 100)
    adder.add(100)
    sw.tick(1.second)
    adder.add(100)
    assert(adder.sum() === 300)
    sw.tick(100.seconds)
    assert(adder.sum() === 0)
  }
}
