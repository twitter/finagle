package com.twitter.finagle.builder

import org.specs.SpecificationWithJUnit

class NonShrinkingClusterSpec extends SpecificationWithJUnit {
  "SkepticalClusterFilter" should {
    val N = 10
    val inCluster = new ClusterInt()
    val outCluster = new NonShrinkingCluster(inCluster)

    "delay Rem events" in {
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
    }

    "pass Rem events after receiving an Add" in {
      val before = (0 until N).toSet
      val after = before map { _ + N }
      before foreach { inCluster.add(_) }

      val (seq, changes) = outCluster.snap
      var set = seq.toSet
      changes foreach { spool =>
        spool foreach {
          case Cluster.Add(elem) => set += elem
          case Cluster.Rem(elem) => set -= elem
        }
      }

      set must be_==(before)
      before foreach { inCluster.del(_) }
      set must be_==(before)
      after foreach { inCluster.add(_) }
      set must be_==(after)
    }

    "cancel Rem changes when receiving an equal Add" in {
      val (_, changes) = outCluster.snap
      var adds, rems = 0
      changes foreach { spool =>
        spool foreach {
          case Cluster.Add(elem) => adds += 1
          case Cluster.Rem(elem) => rems -= 1
        }
      }
      0 until N foreach { inCluster.add(_) }
      adds must be_==(N)

      0 until N foreach { inCluster.del(_) }
      rems must be_==(0)

      adds = 0
      0 until N foreach { inCluster.add(_) }
      adds must be_==(0)
      rems must be_==(0)
    }
  }
}
