package com.twitter.finagle.builder

import com.twitter.finagle.integration.DynamicCluster
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import scala.collection.mutable

@RunWith(classOf[JUnitRunner])
class ClusterTest extends FunSuite {
  case class WrappedInt(value: Int)

  class ClusterHelper {
    val N = 10
    val cluster1 = new DynamicCluster[Int]()
    val cluster2 = cluster1.map(a => WrappedInt(a))
  }

  test("Cluster map should provide 1-1 mapping to the result cluster") {
    val h = new ClusterHelper
    import h._

    0.until(N).foreach { cluster1.add }
    val (seq, changes) = cluster2.snap
    var set = seq.toSet
    changes foreach { spool =>
      spool foreach {
        case Cluster.Add(elem) => set += elem
        case Cluster.Rem(elem) => set -= elem
      }
    }
    assert(set.size == N)
    0.until(N).foreach { cluster1.del }
    assert(set.isEmpty)
  }

  test("Cluster map should remove mapped objects in the same order they were received (for each key)") {
    val h = new ClusterHelper
    import h._

    val changes = mutable.Queue[Cluster.Change[WrappedInt]]()
    val (_, spool) = cluster2.snap
    spool.foreach { _.foreach { changes.enqueue(_) } }
    cluster1.add(1)
    cluster1.add(2)
    cluster1.add(1)
    cluster1.add(2)
    assert(changes == Seq(
      Cluster.Add(WrappedInt(1)),
      Cluster.Add(WrappedInt(2)),
      Cluster.Add(WrappedInt(1)),
      Cluster.Add(WrappedInt(2))
    ))

    cluster1.del(1)
    assert(changes.size == 5)
    assert(changes(4).value == changes(0).value)
    cluster1.del(1)
    assert(changes.size == 6)
    assert(changes(5).value == changes(2).value)

    cluster1.del(2)
    assert(changes.size == 7)
    assert(changes(6).value == changes(1).value)
    cluster1.del(2)
    assert(changes.size == 8)
    assert(changes(7).value == changes(3).value)

    cluster1.del(100)
    assert(changes.size == 9)
    for (ch <- changes take 8)
      assert(ch.value != changes(8).value)
  }

  test("Cluster ready should wait on cluster initialization") {
    val cluster = new DynamicCluster[Int]()
    val ready = cluster.ready
    assert(!ready.isDefined)
    cluster.del(1)
    assert(!ready.isDefined)
    cluster.add(1)
    assert(ready.isDefined)
  }

  test("Cluster ready should always be defined on StaticCluster") {
    val cluster = new StaticCluster[Int](Seq[Int](1, 2))
    assert(cluster.ready.isDefined)
  }
}
