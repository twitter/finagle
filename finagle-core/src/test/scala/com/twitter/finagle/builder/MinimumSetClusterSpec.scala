package com.twitter.finagle.builder

import com.twitter.util.Await
import org.specs.SpecificationWithJUnit

class MinimumSetClusterSpec extends SpecificationWithJUnit {
  "MinimumSetCluster" should {
    val dynamicCluster = new ClusterInt()
    val minimum = Set(1,2,3)

    val cluster = new MinimumSetCluster(minimum, dynamicCluster)

    "initial set is union" in {
      dynamicCluster.add(4)
      cluster.snap._1 must haveSameElementsAs(Seq(1,2,3,4))
    }

    "propagate uncensored updates" in {
      val (_, updates) = cluster.snap
      dynamicCluster.add(4)
      updates.isDefined must beTrue
      Await.result(updates).head mustEqual Cluster.Add(4)
    }

    "don't propagate censored updates" in {
      val (_, updates) = cluster.snap
      dynamicCluster.del(3)
      updates.isDefined must beFalse
    }
  }
}
