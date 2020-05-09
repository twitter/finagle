package com.twitter.finagle.redis

import com.twitter.finagle.Redis
import com.twitter.finagle.redis.protocol.ClusterNode
import com.twitter.finagle.redis.util.{RedisCluster, RedisMode, RedisTestHelper}
import com.twitter.io.Buf
import com.twitter.util.{Await, Future}
import java.net.InetSocketAddress
import org.scalactic.source.Position
import org.scalatest.{BeforeAndAfterAll, Tag}

trait ClusterClientTest extends RedisTest with BeforeAndAfterAll {

  override def test(testName: String, testTags: Tag*)(f: => Any)(implicit pos: Position): Unit = {
    if (RedisTestHelper.redisServerExists) {
      super.test(testName, testTags: _*)(f)(pos)
    } else {
      ignore(testName)(f)(pos)
    }
  }

  val TotalHashSlots: Int = 16384
  val LastHashSlot: Int = TotalHashSlots - 1

  val primaryCount: Int

  val replicasPerPrimary: Int = 1

  lazy val serverCount = primaryCount + primaryCount * replicasPerPrimary

  override def beforeAll(): Unit = {
    RedisCluster.start(count = serverCount, mode = RedisMode.Cluster)
  }

  override def afterAll(): Unit = RedisCluster.stopAll()

  protected def assertSlots(client: ClusterClient, expected: Seq[(Int, Int)]): Unit = {
    val id = Await.result(client.nodeId()).get
    val slots = Await.result(client.slots)

    val slotsOnId = slots.filter(_.master.id == Some(id))
    assert(slotsOnId.size == expected.size)

    val orderedSlots = slotsOnId.map(s => (s.start, s.end)).sorted
    assert(orderedSlots == expected)
  }

  protected def assertEqualInfo(
    clients: Seq[ClusterClient],
    expected: Seq[(String, String)]
  )(
    f: ClusterClient => Future[Map[String, String]]
  ): Unit = {
    for (client <- clients) {
      val info = Await.result(f(client))

      // make sure info contains all (k,v)-pairs from expected
      assert(info.filter(expected.contains(_)).size == expected.size)
    }
  }

  // Starts and configures a cluster with primaryCount*replicasPerPrimary servers
  // - All servers know each other
  // - The list of slot ranges are assigned round robin to the primaries
  //   Start and end of the range are inclusive
  //   Defaults to assigning all slots to the primary with index 0
  protected def startCluster(slots: Seq[(Int, Int)] = Seq()): Unit = {
    val allSlots = if (slots.size == 0) Seq((0, LastHashSlot)) else slots

    // check if all slots are covered
    val coveredSlots = allSlots.map { case (start, end) => (start to end) }.flatten
    assert(coveredSlots.size == TotalHashSlots)
    assert(coveredSlots.sorted == (0 until TotalHashSlots))

    val clients: Seq[ClusterClient] = (0 until serverCount).map(newClusterClient)
    val primaries = clients.slice(0, primaryCount)
    val replicas = clients.slice(primaryCount, clients.size)

    // assign all slots to the primaries (first primaryCount servers)
    for (((start, end), index) <- allSlots.zipWithIndex) {
      Await.result(primaries(index % primaryCount).addSlots((start to end)))
    }

    // let the nodes meet each other
    for ((client, index) <- clients.zipWithIndex) {
      // take the next client in the ring
      val nextServer = RedisCluster.address((index + 1) % clients.size).get
      Await.result(client.meet(nextServer))
    }

    // make sure that all servers know the cluster config
    waitUntilAsserted("Cluster slot assignment and meet completed") {
      val expected = Seq(
        "cluster_known_nodes" -> serverCount.toString,
        // number of primaries that have slots assigned
        "cluster_size" -> Math.min(allSlots.size, primaryCount).toString,
        "cluster_slots_assigned" -> TotalHashSlots.toString,
        "cluster_slots_ok" -> TotalHashSlots.toString
      )

      assertEqualInfo(clients, expected)(_.clusterInfo)
    }

    // assign replicas to their primaries (round-robin)
    for ((client, index) <- replicas.zipWithIndex) {
      val primaryId = Await.result(primaries(index % primaryCount).nodeId())
      assert(primaryId.nonEmpty)
      Await.result(client.replicate(primaryId.get))
    }

    // check that all primaries have replicas connected
    waitUntilAsserted("Primaries are connected with replicas") {
      val expected = Seq("role" -> "master", "connected_slaves" -> replicasPerPrimary.toString)

      assertEqualInfo(primaries, expected)(_.infoMap)
    }

    // check that all replicas are connected to primaries
    waitUntilAsserted("Replicas are connected with primaries") {
      val expected = Seq(
        "role" -> "slave",
        "master_link_status" -> "up"
      )

      assertEqualInfo(replicas, expected)(_.infoMap)
    }

    // Finally, lets wait for the entire cluster to be ok
    waitUntilAsserted("All servers report that the cluster is ok") {
      assertEqualInfo(clients, Seq("cluster_state" -> "ok"))(_.clusterInfo)
    }

    clients.foreach(_.close())
  }

  private def migrateSlotKeys(
    src: ClusterClient,
    destAddr: InetSocketAddress,
    slot: Int
  ): Future[Unit] = {
    def migrateKeys(keys: Seq[Buf]): Future[Unit] = {
      if (keys.size == 0) Future.Unit
      else {
        for {
          _ <- src.migrate(destAddr, keys)
          _ <- migrateSlotKeys(src, destAddr, slot)
        } yield ()
      }
    }

    for {
      keys <- src.getKeysInSlot(slot)
      _ <- migrateKeys(keys)
    } yield ()

  }

  private def reshardSingle(
    a: ClusterClient,
    aId: String,
    b: ClusterClient,
    bNode: ClusterNode
  )(
    slot: Int
  ): Future[Unit] =
    for {
      // The protocol has four steps:
      // https://redis.io/commands/cluster-setslot#redis-cluster-live-resharding-explained

      // 1. We send B: CLUSTER SETSLOT 10 IMPORTING A (propose)
      _ <- b.setSlotImporting(slot, aId)

      // 2. We send A: CLUSTER SETSLOT 10 MIGRATING B (accept)
      _ <- a.setSlotMigrating(slot, bNode.id.get)

      // 3. Migrate data when it exists
      _ <- migrateSlotKeys(a, bNode.addr, slot)

      // 4. Send both A, B: CLUSTER SETSLOT 10 NODE B (commit)
      _ <- a.setSlotNode(slot, bNode.id.get)
      _ <- b.setSlotNode(slot, bNode.id.get)
    } yield ()

  protected def reshard(a: ClusterClient, b: ClusterClient, slots: Seq[Int]): Future[Unit] =
    for {
      aId <- a.nodeId()
      bNode <- b.node()
      _ <- Future.join(slots.map(reshardSingle(a, aId.get, b, bNode.get)))
    } yield ()

  private def newClusterClient(index: Int): ClusterClient = {
    ClusterClient(
      Redis.client.newClient(RedisCluster.hostAddresses(from = index, until = index + 1))
    )
  }

  protected def withClusterClient(index: Int)(testCode: ClusterClient => Any): Unit = {
    val client = newClusterClient(index)
    try { testCode(client) }
    finally { client.close() }
  }

  protected def withClusterClients(indices: Int*)(testCode: Seq[ClusterClient] => Any): Unit = {
    val clients = indices.map(newClusterClient)
    try { testCode(clients) }
    finally { clients.foreach(_.close()) }
  }

}
