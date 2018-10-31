package com.twitter.finagle.redis.integration

import com.twitter.finagle.redis.ClusterClientTest
import com.twitter.finagle.redis.tags.{ClientTest, RedisTest}
import com.twitter.io.Buf
import com.twitter.util.Await

final class ClusterClientReshardingIntegrationSuite extends ClusterClientTest {

  // Generate hash keys/slots for testing:
  // while true; do r=`openssl rand -hex 4`;
  // s=`echo "cluster keyslot $r" | redis-cli | cut -f2`; echo "$s,$r"; done

  val primaryCount: Int = 2

  // Resharding is depending on a correctly started/configured cluster
  test("Cluster is configured and started correctly", RedisTest, ClientTest) {
    startCluster()
  }

  test("Correctly hand over a slot to another primary using SETSLOT", RedisTest, ClientTest) {
    withClusterClients(0, 1) {
      case Seq(a, b) =>
        val key = Buf.Utf8("6ff70029")
        val value = Buf.Utf8("foo")

        Await.result(a.set(key, value))

        // hand over slot 42 from a to b
        Await.result(reshard(a, b, Seq(42)))

        // B is responsible for slot 42
        waitUntilAsserted("A is responsible for slot 0-41, 43-16383 and B for 42") {
          assertSlots(a, Seq((0, 41), (43, LastHashSlot)))
          assertSlots(b, Seq((42, 42)))
        }
    }
  }
}
