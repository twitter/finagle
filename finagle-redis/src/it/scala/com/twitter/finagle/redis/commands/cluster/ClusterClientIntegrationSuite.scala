package com.twitter.finagle.redis.integration

import com.twitter.conversions.time._
import com.twitter.finagle.redis.{ClusterClientTest, ServerError}
import com.twitter.finagle.redis.tags.{ClientTest, RedisTest}
import com.twitter.finagle.redis.util.{BufToString, RedisCluster}
import com.twitter.finagle.util.DefaultTimer
import com.twitter.io.Buf
import com.twitter.logging.Logger
import com.twitter.util.{Await, Awaitable, Future, Time}
import org.junit.Ignore
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@Ignore
@RunWith(classOf[JUnitRunner])
final class ClusterClientIntegrationSuite extends ClusterClientTest {

  val primaryCount: Int = 3

  test("Correctly retrieve CLUSTER INFO", RedisTest, ClientTest) {
    withClusterClient(0) { client =>
      val info = Await.result(client.clusterInfo)

      // cluster info has 11 entries
      assert(info.size == 11)
      assert(info.contains("cluster_size"))
    }
  }

  test("Correctly retrieve empty slots using SLOTS", RedisTest, ClientTest) {
    withClusterClient(0) { client =>
      val slots = Await.result(client.slots)

      assert(slots == Seq())
    }
  }

  test("Correctly assign slots to a server using ADDSLOTS", RedisTest, ClientTest) {
    withClusterClient(0) { client =>
      Await.result(client.addSlots((0 until 100)))

      val info = Await.result(client.clusterInfo)

      assert(info.get("cluster_slots_assigned") == Some("100"))
    }
  }

  test("Correctly retrieve assigned slots using SLOTS", RedisTest, ClientTest) {
    withClusterClient(0) { client =>
      val slots = Await.result(client.slots)

      assert(slots.size == 1)
      val s = slots.head
      assert(s.start == 0 && s.end == 99 && s.master.addr.getHostName == "localhost" && s.replicas.size == 0)
    }
  }

  test("Return an error when using REPLICATE on a replica that does not know the primary", RedisTest, ClientTest) {
    withClusterClients(0, primaryCount) { case Seq(primary, replica) =>
      // select the first backup as a replica
      val primaryId = Await.result(primary.nodeId())

      assert(primaryId.nonEmpty)

      // should throw a server error since the replica does not yet know the primary
      val error = intercept[ServerError](Await.result(replica.replicate(primaryId.get)))

      assert(error.getMessage == (s"ERR Unknown node ${primaryId.get}"))
    }
  }

  test("Correctly MEET a primary and a replica", RedisTest, ClientTest) {
    withClusterClients(0, primaryCount) { case Seq(primary, replica) =>
      // select the first backup as a replica
      val replicaAddr = RedisCluster.address(primaryCount).get

      Await.result(primary.meet(replicaAddr))

      waitUntilAsserted("Cluster size updated") {
        val primaryInfo = Await.result(primary.clusterInfo)
        val replicaInfo = Await.result(replica.clusterInfo)
        assert(primaryInfo.get("cluster_known_nodes") == Some("2"))
        assert(replicaInfo.get("cluster_known_nodes") == Some("2"))

        // cluster is still of size 1 since it includes primaries only
        assert(primaryInfo.get("cluster_size") == Some("1"))
        assert(replicaInfo.get("cluster_size") == Some("1"))
      }
    }
  }


  test("Correctly assign a node as replica using REPLICATE", RedisTest, ClientTest) {
    withClusterClients(0, primaryCount) { case Seq(primary, replica) =>
      // select the first backup as a replica
      val primaryId = Await.result(primary.nodeId())

      assert(primaryId.nonEmpty)

      Await.result(replica.replicate(primaryId.get))

      waitUntilAsserted("Replication complete") {
        val primaryInfo = Await.result(primary.infoMap())
        assert(primaryInfo.get("role") == Some("master"))
        assert(primaryInfo.get("connected_slaves") == Some("1"))

        val replicaInfo = Await.result(replica.infoMap())
        assert(replicaInfo.get("role") == Some("slave"))
      }
    }
  }


}
