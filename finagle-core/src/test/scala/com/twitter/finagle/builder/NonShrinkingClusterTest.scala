package com.twitter.finagle.builder

import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

@RunWith(classOf[JUnitRunner])
class NonShrinkingClusterTest extends FunSuite {
  class ClusterHelper {
    val N = 10
    val inCluster = new ClusterInt()
    val outCluster = new NonShrinkingCluster(inCluster)
  }

  test("NonShrinkingClusterTest should delay Rem events") {
    val h = new ClusterHelper
    import h._

    0 until N foreach { inCluster.add(_) }
    val (seq, changes) = outCluster.snap
    var set = seq.toSet
    changes foreach { spool =>
      spool foreach {
        case Cluster.Add(elem) => set += elem
        case Cluster.Rem(elem) => set -= elem
      }
    }
    assert(set.size == N)
    0 until N foreach { inCluster.del(_) }
    assert(set.size == N)
  }

  test("NonShrinkingClusterTest should pass Rem events after receiving an Add") {
    val h = new ClusterHelper
    import h._

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

    assert(set == before)
    before foreach { inCluster.del(_) }
    assert(set == before)
    after foreach { inCluster.add(_) }
    assert(set == after)
  }

  test("NonShrinkingClusterTest should cancel Rem changes when receiving an equal Add") {
    val h = new ClusterHelper
    import h._

    val (_, changes) = outCluster.snap
    var adds, rems = 0
    changes foreach { spool =>
      spool foreach {
        case Cluster.Add(elem) => adds += 1
        case Cluster.Rem(elem) => rems -= 1
      }
    }
    0 until N foreach { inCluster.add(_) }
    assert(adds == N)

    0 until N foreach { inCluster.del(_) }
    assert(rems == 0)

    adds = 0
    0 until N foreach { inCluster.add(_) }
    assert(adds == 0)
    assert(rems == 0)
  }

}
