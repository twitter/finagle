package com.twitter.finagle.builder

import com.twitter.util.MockTimer
import com.twitter.util.Time
import org.specs.SpecificationWithJUnit
import com.twitter.conversions.time._
import scala.collection._
import scala.util.Random

class LingeringClusterSpec extends SpecificationWithJUnit {
  "LingeringCluster" should {
    val N = 10
    val inCluster = new ClusterInt()
    val timer = new MockTimer()
    val outCluster = new LingeringCluster(inCluster, 10.seconds, timer)

    "delay Rem events" in Time.withCurrentTimeFrozen { tc =>
      0 until N foreach { inCluster.add(_) }
      val (seq, changes) = outCluster.snap

      var set = seq.toSet
      changes foreach { spool =>
        spool foreach {
          case Cluster.Add(elem) => set += elem
          case Cluster.Rem(elem) => set -= elem
        }
      }

      set.size must be_==(N)
      0 until N foreach { inCluster.del(_) }
      set.size must be_==(N)

      tc.advance(10.seconds)
      timer.tick()

      set.size must be_==(0)
    }

    "report Add once if the same element is removed and then added again" in Time.withCurrentTimeFrozen { tc =>
      val (seq, changes) = outCluster.snap
      var adds, rems = 0
      changes foreach { spool =>
        spool foreach {
          case Cluster.Add(elem) => adds += 1
          case Cluster.Rem(elem) => rems -= 1
        }
      }

      0 until N foreach { inCluster.add(_) }
      adds must be_==(N)
      rems must be_==(0)

      tc.advance(1.second); timer.tick()

      0 until N foreach { inCluster.del(_) }
      adds must be_==(N)
      rems must be_==(0)

      tc.advance(1.second); timer.tick()

      0 until N foreach { inCluster.add(_) }
      adds must be_==(N)
      rems must be_==(0)
    }

    "handle multiple additions and deletions" in Time.withCurrentTimeFrozen { tc =>
      // here we simulate a DNS service that on each iteration
      // resolves 4 "addresses" out of 10

      val rnd = new Random
      def rndSet = Seq.fill(4) { rnd.nextInt(10) } toSet
      val (seq, changes) = outCluster.snap

      var set = seq.toSet
      changes foreach { spool =>
        spool foreach {
          case Cluster.Add(elem) => set += elem
          case Cluster.Rem(elem) => set -= elem
        }
      }

      var prevSet = Set.empty[Int]

      for (i <- 0 to 100) {
        val newSet = rndSet
        val added = newSet &~ prevSet
        val removed = prevSet &~ newSet

        added foreach { inCluster.add(_) }
        removed foreach { inCluster.del(_) }

        set must be_== (prevSet ++ newSet)

        tc.advance(5.seconds)
        timer.tick()

        set must be_== (prevSet ++ newSet)

        tc.advance(5.seconds)
        timer.tick()

        set must be_== (newSet)

        prevSet = newSet
      }
    }

  }
}
