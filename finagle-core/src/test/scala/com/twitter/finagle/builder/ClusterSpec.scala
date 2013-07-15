package com.twitter.finagle.builder

import com.twitter.conversions.time._
import com.twitter.finagle.GlobalRequestTimeoutException
import com.twitter.finagle.integration.{DynamicCluster, StringCodec}
import com.twitter.util.Await
import java.net.SocketAddress
import org.specs.SpecificationWithJUnit
import scala.collection.mutable

class ClusterSpec extends SpecificationWithJUnit {
  case class WrappedInt(val value: Int)

  "Cluster map" should {
    val N = 10
    val cluster1 = new DynamicCluster[Int]()
    val cluster2 = cluster1.map(a => WrappedInt(a))

    "provide 1-1 mapping to the result cluster" in {
      0 until N foreach { cluster1.add(_) }
      val (seq, changes) = cluster2.snap
      var set = seq.toSet
      changes foreach { spool =>
        spool foreach {
          case Cluster.Add(elem) => set += elem
          case Cluster.Rem(elem) => set -= elem
        }
      }
      set.size must be_==(N)
      0 until N foreach { cluster1.del(_) }
      set.size must be_==(0)
    }

    "remove mapped objects in the same order they were received (for each key)" in {
      val changes = mutable.Queue[Cluster.Change[WrappedInt]]()
      val (_, spool) = cluster2.snap
      spool foreach { _ foreach { changes enqueue _ } }
      cluster1.add(1)
      cluster1.add(2)
      cluster1.add(1)
      cluster1.add(2)
      changes.toSeq must be_==(Seq(
        Cluster.Add(WrappedInt(1)),
        Cluster.Add(WrappedInt(2)),
        Cluster.Add(WrappedInt(1)),
        Cluster.Add(WrappedInt(2))
      ))

      cluster1.del(1)
      changes must haveSize(5)
      changes(4).value must be(changes(0).value)
      cluster1.del(1)
      changes must haveSize(6)
      changes(5).value must be(changes(2).value)

      cluster1.del(2)
      changes must haveSize(7)
      changes(6).value must be(changes(1).value)
      cluster1.del(2)
      changes must haveSize(8)
      changes(7).value must be(changes(3).value)

      cluster1.del(100)
      changes must haveSize(9)
      for (ch <- changes take 8)
        ch.value mustNot be(changes(8).value)
    }
  }

  "Cluster ready" should {
    "wait on cluster initialization" in {
      val cluster = new DynamicCluster[Int]()
      val ready = cluster.ready
      ready.isDefined must beFalse
      cluster.del(1)
      ready.isDefined must beFalse
      cluster.add(1)
      ready.isDefined must beTrue
    }

    "always be defined on StaticCluster" in {
      val cluster = new StaticCluster[Int](Seq[Int](1, 2))
      cluster.ready.isDefined must beTrue
    }

    // Cluster initialization should honor global timeout as well as timeout specified
    // together with the requests
    "honor timeout while waiting for cluster to initialize" in {
      val cluster = new DynamicCluster[SocketAddress](Seq[SocketAddress]()) //empty cluster
      val client = ClientBuilder()
          .cluster(cluster)
          .codec(StringCodec)
          .hostConnectionLimit(1)
          .timeout(1.seconds) //global time out
          .build()

      Await.result(client("hello1")) must throwA[GlobalRequestTimeoutException]

      // It also should honor timeout specified with the request
      Await.result(client("hello2"), 10.milliseconds) must throwA[com.twitter.util.TimeoutException]
    }
  }
}
