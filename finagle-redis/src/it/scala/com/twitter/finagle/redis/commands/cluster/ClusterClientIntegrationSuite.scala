package com.twitter.finagle.redis.integration

import com.twitter.finagle.redis.{ClusterClientTest, ServerError}
import com.twitter.finagle.redis.tags.{ClientTest, RedisTest}
import com.twitter.finagle.redis.util.RedisCluster
import com.twitter.util.Await

final class ClusterClientIntegrationSuite extends ClusterClientTest {

  // Generate hash keys/slots for testing:
  // while true; do r=`openssl rand -hex 4`;
  // s=`echo "cluster keyslot $r" | redis-cli | cut -f2`; echo "$s,$r"; done

  val primaryCount: Int = 2

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
      // assign a single slot
      Await.result(client.addSlots(Seq(0)))
      val slotSingle = Await.result(client.slots)
      assert(slotSingle.size == 1)
      assert(slotSingle.head.start == 0 && slotSingle.head.end == 0)

      Await.result(client.addSlots((1 until 100)))
      val slotRange = Await.result(client.slots)
      assert(slotRange.size == 1)
      assert(slotRange.head.start == 0 && slotRange.head.end == 99)

      val info = Await.result(client.clusterInfo)

      assert(info.get("cluster_slots_assigned") == Some("100"))
    }
  }

  test("Correctly retrieve assigned slots using SLOTS", RedisTest, ClientTest) {
    withClusterClient(0) { client =>
      val slots = Await.result(client.slots)

      assert(slots.size == 1)
      val s = slots.head
      assert(
        s.start == 0 && s.end == 99 && s.master.addr.getHostName == "localhost" && s.replicas.size == 0
      )
    }
  }

  test(
    "Return an error when using REPLICATE on a replica that does not know the primary",
    RedisTest,
    ClientTest
  ) {
    withClusterClients(0, primaryCount) {
      case Seq(primary, replica) =>
        // select the first backup as a replica
        val primaryId = Await.result(primary.nodeId())

        assert(primaryId.nonEmpty)

        // should throw a server error since the replica does not yet know the primary
        val error = intercept[ServerError](Await.result(replica.replicate(primaryId.get)))

        assert(error.getMessage == (s"ERR Unknown node ${primaryId.get}"))
    }
  }

  test("Correctly MEET a primary and a replica", RedisTest, ClientTest) {
    withClusterClients(0, primaryCount) {
      case Seq(primary, replica) =>
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

  test("Correctly return the list of nodes with NODES", RedisTest, ClientTest) {
    withClusterClients(0, primaryCount) {
      case Seq(primary, replica) =>
        // select the first backup as a replica
        val primaryNodes = Await.result(primary.nodes())
        val replicaNodes = Await.result(replica.nodes())

        assert(primaryNodes.size == 2)
        assert(replicaNodes.size == 2)
        assert(
          primaryNodes.filter(_.isMyself).head.id !=
            replicaNodes.filter(_.isMyself).head.id
        )
    }
  }

  test("Correctly assign a node as replica using REPLICATE", RedisTest, ClientTest) {
    withClusterClients(0, primaryCount) {
      case Seq(primary, replica) =>
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
