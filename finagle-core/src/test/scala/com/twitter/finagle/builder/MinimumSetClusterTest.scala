package com.twitter.finagle.builder

import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import com.twitter.util.Await

@RunWith(classOf[JUnitRunner])
class MinimumSetClusterTest extends FunSuite{
  class helper {
    val dynamicCluster = new ClusterInt()
    val minimum = Set(1,2,3)

    val cluster = new MinimumSetCluster(minimum, dynamicCluster)
  }

  test("initial set is union"){
    val h = new helper
    import h._

    dynamicCluster.add(4)
    assert(cluster.snap._1 == Seq(1, 2, 3, 4))
  }

  test("propagate uncensored updates"){
    val h = new helper
    import h._

    val (_, updates) = cluster.snap
    dynamicCluster.add(4)
    assert(updates.isDefined)
    assert(Await.result(updates).head == Cluster.Add(4))
  }

  test("don't propagate censored updates"){
    val h = new helper
    import h._

    val (_, updates) = cluster.snap
    dynamicCluster.del(3)
    assert(!updates.isDefined)
  }
}
