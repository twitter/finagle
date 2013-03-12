package com.twitter.finagle.exp

import com.twitter.conversions.time._
import com.twitter.util.Duration
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class LatencyHistogramTest extends FunSuite {
  val range = 10.seconds

  def testRandom(rng: Random, N: Int) {
    val histo = new LatencyHistogram(range)
    val input = Array.fill(N) { 
      Duration.fromMilliseconds(rng.nextInt).abs % range
    }
    for (d <- input)
      histo.add(d)
    val sorted = input.sorted
    for (q <- 0 until 100)
      assert(histo.quantile(q) === sorted(q*N/100))
  }
  
  val tests = Seq(
    (130827L, 300),
    (130655L, 200),
    (127290L, 800),
    (128163L, 123)
  )
  
  for ((seed, n) <- tests)
    test("random: %d %d".format(seed, n)) {
      testRandom(new Random(seed), n)
    }
}
