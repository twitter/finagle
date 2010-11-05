package com.twitter.finagle.util

import org.specs.Specification

import com.twitter.util.Time
import com.twitter.util.TimeConversions._

object SampleSpec extends Specification {
  "scalar samples" should {
    "count!" in {
      val c = new ScalarSample
      c.sum must be_==(0)
      c.add(1)
      c.add(1)
      c.sum must be_==(2)

      c.add(1, 1000)
      c.sum must be_==(3)
      c.count must be_==(1002)
    }

    "compute means" in {
      val c = new ScalarSample
      for (i <- 1 to 100)
        c.add(i)

      c.count must be_==(100)
      c.sum must be_==(50 * (1+100))
      c.mean must be_==(50)
    }
  }

  "time windowed counter" should {
    Time.freeze()

    "keep a total sum over its window" in {
      val c = new TimeWindowedSample[ScalarSample](10, 10.seconds)
      c.sum must be_==(0)
      c.add(1, 1)
      c.sum must be_==(1)
      c.count must be_==(1)

      Time.advance(11.seconds)
      c.add(1)
      c.sum must be_==(2)
      c.count must be_==(2)

      Time.advance(80.seconds)
      c.add(1)
      c.sum must be_==(3)
      c.count must be_==(3)

      Time.advance(10.seconds)
      c.sum must be_==(2)
      c.count must be_==(2)
      c.add(1)
      c.sum must be_==(3)
      c.count must be_==(3)
    }

    "keep a total sum over its window (2)" in {
      val c = new TimeWindowedSample[ScalarSample](10, 10.seconds)

      for (i <- 1 to 100) {
        c.add(1, 1)
        c.sum must be_==(i)
        Time.advance(1.seconds)
      }

      c.sum must be_==(100)

      for (i <- 0 until 10) {
        Time.advance(10.seconds)
        c.add(10)
        c.sum must be_==(100)
      }
    }

    "compute rate" in {
      val c = new TimeWindowedSample[ScalarSample](10, 10.seconds)
      c.add(1)
      c.rateInHz() must be_==(0)

      Time.advance(50.seconds)
      for (i <- 0 until 100) {
        c.rateInHz() must be_==(i)
        c.add(60, 60)
      }
    }
  }

  "sample trees" should {
    val n11 = new ScalarSample
    val n12 = new ScalarSample
    val n21 = new ScalarSample
    val n22 = new ScalarSample

    val tree = SampleNode(
      "root", Seq(
        SampleNode("n1", Seq(SampleLeaf("n11", n11), SampleLeaf("n12", n12))),
        SampleNode("n2", Seq(SampleLeaf("n21", n21), SampleLeaf("n22", n22)))
      )
    )

    "aggregate up the tree" in {
      tree.count must be_==(0)
      n11.add(123)
      tree.count must be_==(1)
      tree.mean  must be_==(123)

      n21.add(123*2)
      tree.count must be_==(2)
      tree.mean  must be_==((123 * 3) / 2)

      val SampleNode(_, Seq(n1, _)) = tree
      n1.count must be_==(1)
      n1.mean must be_==(123)
    }

    "print a nice tree" in {
      for (i <- 0 until 100; n <- Seq(n11, n12, n21, n22))
        n.add(i)

      tree.toString must be_==(
        """root [count=400, sum=19800, mean=49]
          |_n1 [count=200, sum=9900, mean=49]
          |__n11 [count=100, sum=4950, mean=49]
          |__n12 [count=100, sum=4950, mean=49]
          |_n2 [count=200, sum=9900, mean=49]
          |__n21 [count=100, sum=4950, mean=49]
          |__n22 [count=100, sum=4950, mean=49]""" stripMargin)
    }
  }
}
