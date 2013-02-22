package com.twitter.finagle.memcached.integration

import _root_.java.io.ByteArrayOutputStream
import _root_.java.net.InetSocketAddress
import com.twitter.common.application.ShutdownRegistry.ShutdownRegistryImpl
import com.twitter.common.zookeeper.ServerSet.EndpointStatus
import com.twitter.common.zookeeper.{ZooKeeperUtils, ServerSets, ZooKeeperClient}
import com.twitter.common.zookeeper.testing.ZooKeeperTestServer
import com.twitter.concurrent.Spool
import com.twitter.concurrent.Spool.*::
import com.twitter.conversions.time._
import com.twitter.finagle.builder.{Cluster, ClientBuilder}
import com.twitter.finagle.memcached._
import com.twitter.finagle.memcached.protocol._
import com.twitter.finagle.memcached.protocol.text.Memcached
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import com.twitter.finagle.stats.SummarizingStatsReceiver
import com.twitter.finagle.zookeeper.ZookeeperServerSetCluster
import com.twitter.thrift.Status
import com.twitter.util.Future
import org.jboss.netty.util.CharsetUtil
import org.specs.SpecificationWithJUnit
import scala.collection.mutable

class ClientSpec extends SpecificationWithJUnit {
  "ConnectedClient" should {
    /**
     * Note: This integration test requires a real Memcached server to run.
     */
    var client: Client = null

    val stats = new SummarizingStatsReceiver

    doBefore {
      var address: Option[InetSocketAddress] = ExternalMemcached.start()
      if (address == None) skip("Cannot start memcached. Skipping...")
      val service = ClientBuilder()
        .hosts(Seq(address.get))
        .codec(new Memcached(stats))
        .hostConnectionLimit(1)
        .build()
      client = Client(service)
    }

    doAfter {
      ExternalMemcached.stop()
    }

    "simple client" in {
      "set & get" in {
        client.delete("foo")()
        client.get("foo")() mustEqual None
        client.set("foo", "bar")()
        client.get("foo")().get.toString(CharsetUtil.UTF_8) mustEqual "bar"
      }

      "get" in {
        client.set("foo", "bar")()
        client.set("baz", "boing")()
        val result = client.get(Seq("foo", "baz", "notthere"))()
          .map { case (key, value) => (key, value.toString(CharsetUtil.UTF_8)) }
        result mustEqual Map(
          "foo" -> "bar",
          "baz" -> "boing"
        )
      }

      "gets" in {
        client.set("foos", "xyz")()
        client.set("bazs", "xyz")()
        client.set("bazs", "zyx")()
        val result = client.gets(Seq("foos", "bazs", "somethingelse"))()
          .map { case (key, (value, casUnique)) =>
            (key, (value.toString(CharsetUtil.UTF_8), casUnique.toString(CharsetUtil.UTF_8)))
          }

        result mustEqual Map(
          "foos" -> ("xyz", "1"),  // the "cas unique" values are predictable from a fresh memcached
          "bazs" -> ("zyx", "3")
        )
      }

      "cas" in {
        client.set("x", "y")()
        val Some((value, casUnique)) = client.gets("x")()
        value.toString(CharsetUtil.UTF_8) must be_==("y")
        casUnique.toString(CharsetUtil.UTF_8) must be_==("1")

        client.cas("x", "z", "2")() must beFalse
        client.cas("x", "z", casUnique)() must beTrue
        val res = client.get("x")()
        res must beSomething
        res.get.toString(CharsetUtil.UTF_8) must be_==("z")
      }

      "append & prepend" in {
        client.set("foo", "bar")()
        client.append("foo", "rab")()
        client.get("foo")().get.toString(CharsetUtil.UTF_8) mustEqual "barrab"
        client.prepend("foo", "rab")()
        client.get("foo")().get.toString(CharsetUtil.UTF_8) mustEqual "rabbarrab"
      }

      "incr & decr" in {
        // As of memcached 1.4.8 (issue 221), empty values are no longer treated as integers
        client.set("foo", "0")()
        client.incr("foo")()    mustEqual Some(1L)
        client.incr("foo", 2)() mustEqual Some(3L)
        client.decr("foo")()    mustEqual Some(2L)

        client.set("foo", "0")()
        client.incr("foo")()    mustEqual Some(1L)
        val l = 1L << 50
        client.incr("foo", l)() mustEqual Some(l + 1L)
        client.decr("foo")()    mustEqual Some(l)
        client.decr("foo", l)() mustEqual Some(0L)
      }

      "stats" in {
        val stats = client.stats()()
        stats must notBeEmpty
        stats.foreach { stat =>
          stat must startWith("STAT")
        }
      }

      "send malformed keys" in {
        // test key validation trait
        client.get("fo o")() must throwA[ClientError]
        client.set("", "bar")() must throwA[ClientError]
        client.get("    foo")() must throwA[ClientError]
        client.get("foo   ")() must throwA[ClientError]
        client.get("    foo")() must throwA[ClientError]
        val nullString: String = null
        client.get(nullString)() must throwA[ClientError]
        client.set(nullString, "bar")() must throwA[ClientError]
        client.set("    ", "bar")() must throwA[ClientError]
        client.set("\t", "bar")() mustNot throwA[ClientError] // \t is allowed
        client.set("\r", "bar")() must throwA[ClientError]
        client.set("\n", "bar")() must throwA[ClientError]
        client.set("\0", "bar")() must throwA[ClientError]

        val veryLongKey = "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz"
        client.get(veryLongKey)() must throwA[ClientError]
        client.set(veryLongKey, "bar").isThrow mustBe true

        // test other keyed command validation
        val nullSeq:Seq[String] = null
        client.get(nullSeq)() must throwA[ClientError]
        client.gets(nullSeq)() must throwA[ClientError]
        client.gets(Seq(null))() must throwA[ClientError]
        client.gets(Seq(""))() must throwA[ClientError]
        client.gets(Seq("foos", "bad key", "somethingelse"))() must throwA[ClientError]
        client.append("bad key", "rab")() must throwA[ClientError]
        client.prepend("bad key", "rab")() must throwA[ClientError]
        client.replace("bad key", "bar")() must throwA[ClientError]
        client.add("bad key", "2")() must throwA[ClientError]
        client.cas("bad key", "z", "2")() must throwA[ClientError]
        client.incr("bad key")() must throwA[ClientError]
        client.decr("bad key")() must throwA[ClientError]
        client.delete("bad key")() must throwA[ClientError]
      }

    }

    "ketama client" in {
      /**
       * We already proved above that we can hit a real memcache server,
       * so we can use our own for the partitioned client test.
       */
      var server1: Server = null
      var server2: Server = null
      var address1: InetSocketAddress = null
      var address2: InetSocketAddress = null

      doBefore {
        server1 = new Server(new InetSocketAddress(0))
        address1 = server1.start().localAddress.asInstanceOf[InetSocketAddress]
        server2 = new Server(new InetSocketAddress(0))
        address2 = server2.start().localAddress.asInstanceOf[InetSocketAddress]
      }

      doAfter {
        server1.stop()
        server2.stop()
      }


      "doesn't blow up" in {
        val client = KetamaClientBuilder()
          .nodes("localhost:%d,localhost:%d".format(address1.getPort, address2.getPort))
          .build()

        client.delete("foo")()
        client.get("foo")() mustEqual None
        client.set("foo", "bar")()
        client.get("foo")().get.toString(CharsetUtil.UTF_8) mustEqual "bar"
      }

      "even in future pool" in {
        lazy val client = KetamaClientBuilder()
            .nodes("localhost:%d,localhost:%d".format(address1.getPort, address2.getPort))
            .build()

        val futureResult = Future.value(true) flatMap {
          _ => client.get("foo")
        }

        futureResult() mustEqual None
      }
    }
  }

  "Cluster client" should {

    /**
     * Note: This integration test requires a real Memcached server to run.
     */
    var shutdownRegistry = new ShutdownRegistryImpl
    var memcachedAddress = List[Option[InetSocketAddress]]()

    var zkServerSetCluster: ZookeeperServerSetCluster = null
    var zookeeperClient: ZooKeeperClient = null
    val zkPath = "/cache/test/silly-cache"
    var zookeeperServer: ZooKeeperTestServer = null

    doBefore {
      // start zookeeper server and create zookeeper client
      zookeeperServer = new ZooKeeperTestServer(0, shutdownRegistry)
      zookeeperServer.startNetwork()

      // connect to zookeeper server
      zookeeperClient = zookeeperServer.createClient(ZooKeeperClient.digestCredentials("user","pass"))

      // create serverset
      val serverSet = ServerSets.create(zookeeperClient, ZooKeeperUtils.EVERYONE_READ_CREATOR_ALL, zkPath)
      zkServerSetCluster = new ZookeeperServerSetCluster(serverSet)

      // start five memcached server and join the cluster
      (0 to 4) foreach { _ =>
        val address: Option[InetSocketAddress] = ExternalMemcached.start()
        if (address == None) skip("Cannot start memcached. Skipping...")
        memcachedAddress ::= address
        zkServerSetCluster.join(address.get)
      }

      // set cache pool config node data
      val cachePoolConfig: CachePoolConfig = new CachePoolConfig(cachePoolSize = 5)
      val output: ByteArrayOutputStream = new ByteArrayOutputStream
      CachePoolConfig.jsonCodec.serialize(cachePoolConfig, output)
      zookeeperClient.get().setData(zkPath, output.toByteArray, -1)

      // a separate client which only does zk discovery for integration test
      zookeeperClient = zookeeperServer.createClient(ZooKeeperClient.digestCredentials("user","pass"))
    }

    doAfter {
      zookeeperClient.close()

      // shutdown zookeeper server and client
      shutdownRegistry.execute()

      // shutdown memcached server
      ExternalMemcached.stop()
    }

    "Simple ClusterClient using finagle load balancing" in {
      "many keys" in {
        // create simple cluster client
        val mycluster =
          new ZookeeperServerSetCluster(
            ServerSets.create(zookeeperClient, ZooKeeperUtils.EVERYONE_READ_CREATOR_ALL, zkPath))
        mycluster.ready() // give it sometime for the cluster to get the initial set of memberships
        val client = Client(mycluster)

        val count = 100
        (0 until count).foreach{
          n => {
            client.set("foo"+n, "bar"+n)
          }
        }

        val tmpClients = memcachedAddress map {
          case(addr) =>
            Client(
              ClientBuilder()
                .hosts(addr.get)
                .codec(new Memcached)
                .hostConnectionLimit(1)
                .build())
        }

        (0 until count).foreach {
          n => {
            var found = false
            tmpClients foreach {
              c =>
                if (c.get("foo"+n)()!=None) {
                  found mustNotBe true
                  found = true
                }
            }
            found mustBe true
          }
        }
      }

    }

    /**
     * Temporarily disable this integration tests due its unstableness.
     * Fix is tracked by CACHE-609

    "Cache specific cluster" in {
      "add and remove" in {
        // the cluster initially must have 5 members
        val myPool = initializePool(5)

        var additionalServers = List[EndpointStatus]()

        /***** start 5 more memcached servers and join the cluster ******/
        // cache pool should remain the same size at this moment
        expectPoolStatus(myPool, currentSize = 5, expectedPoolSize = -1, expectedAdd = -1, expectedRem = -1) {
          additionalServers = addMoreServers(5)
        }.get(2.seconds)() must throwA[com.twitter.util.TimeoutException]

        // update config data node, which triggers the pool update
        // cache pool cluster should be updated
        expectPoolStatus(myPool, currentSize = 5, expectedPoolSize = 10, expectedAdd = 5, expectedRem = 0) {
          updateCachePoolConfigData(10)
        }.get(10.seconds)() mustNot throwA[Exception]

        /***** remove 2 servers from the zk serverset ******/
        // cache pool should remain the same size at this moment
        expectPoolStatus(myPool, currentSize = 10, expectedPoolSize = -1, expectedAdd = -1, expectedRem = -1) {
          additionalServers(0).update(Status.DEAD)
          additionalServers(1).update(Status.DEAD)
        }.get(2.seconds)() must throwA[com.twitter.util.TimeoutException]

        // update config data node, which triggers the pool update
        // cache pool should be updated
        expectPoolStatus(myPool, currentSize = 10, expectedPoolSize = 8, expectedAdd = 0, expectedRem = 2) {
          updateCachePoolConfigData(8)
        }.get(10.seconds)() mustNot throwA[Exception]

        /***** remove 2 more then add 3 ******/
        // cache pool should remain the same size at this moment
        expectPoolStatus(myPool, currentSize = 8, expectedPoolSize = -1, expectedAdd = -1, expectedRem = -1) {
          additionalServers(2).update(Status.DEAD)
          additionalServers(3).update(Status.DEAD)
          addMoreServers(3)
        }.get(2.seconds)() must throwA[com.twitter.util.TimeoutException]

        // update config data node, which triggers the pool update
        // cache pool should be updated
        expectPoolStatus(myPool, currentSize = 8, expectedPoolSize = 9, expectedAdd = 3, expectedRem = 2) {
          updateCachePoolConfigData(9)
        }.get(10.seconds)() mustNot throwA[Exception]
      }

      "zk failures test" in {
        // the cluster initially must have 5 members
        val myPool = initializePool(5)

        /***** fail the server here to verify the pool manager will re-establish ******/
        // cache pool cluster should remain the same
        expectPoolStatus(myPool, currentSize = 5, expectedPoolSize = -1, expectedAdd = -1, expectedRem = -1) {
          zookeeperServer.expireClientSession(zookeeperClient)
          zookeeperServer.shutdownNetwork()
        }.get(2.seconds)() must throwA[com.twitter.util.TimeoutException]

        /***** start the server now ******/
        // cache pool cluster should remain the same
        expectPoolStatus(myPool, currentSize = 5, expectedPoolSize = -1, expectedAdd = -1, expectedRem = -1) {
          zookeeperServer.startNetwork
        }.get(2.seconds)() must throwA[com.twitter.util.TimeoutException]

        /***** start 5 more memcached servers and join the cluster ******/
        // update config data node, which triggers the pool update
        // cache pool cluster should still be able to see undelrying pool changes
        expectPoolStatus(myPool, currentSize = 5, expectedPoolSize = 10, expectedAdd = 5, expectedRem = 0) {
          addMoreServers(5)
          updateCachePoolConfigData(10)
        }.get(10.seconds)() mustNot throwA[Exception]
      }

      "using backup pools" in {
        // shutdown the server before initializing our cache pool cluster
        zookeeperServer.shutdownNetwork()

        // the cache pool cluster should pickup backup pools
        // the underlying pool will continue trying to connect to zk
        val myPool = initializePool(2, Some(scala.collection.immutable.Set(
            new CacheNode("host1", 11211, 1),
            new CacheNode("host2", 11212, 1))))

        // bring the server back online
        // give it some time we should see the cache pool cluster pick up underlying pool
        expectPoolStatus(myPool, currentSize = 2, expectedPoolSize = 5, expectedAdd = 5, expectedRem = 2) {
          zookeeperServer.startNetwork
        }.get(10.seconds)() mustNot throwA[Exception]

        /***** start 5 more memcached servers and join the cluster ******/
        // update config data node, which triggers the pool update
        // cache pool cluster should still be able to see undelrying pool changes
        expectPoolStatus(myPool, currentSize = 5, expectedPoolSize = 10, expectedAdd = 5, expectedRem = 0) {
          addMoreServers(5)
          updateCachePoolConfigData(10)
        }.get(10.seconds)() mustNot throwA[Exception]
      }
    }

    "Ketama ClusterClient using a distributor" in {
      "set & get" in {
        // create my cluster client solely based on a zk client and a path
        val mycluster = CachePoolCluster.newZkCluster(zkPath, zookeeperClient)
        mycluster.ready() // give it sometime for the cluster to get the initial set of memberships

        val client = KetamaClientBuilder()
                .cachePoolCluster(mycluster)
                .build()

        client.delete("foo")()
        client.get("foo")() mustEqual None
        client.set("foo", "bar")()
        client.get("foo")().get.toString(CharsetUtil.UTF_8) mustEqual "bar"
      }

      "many keys" in {
        // create my cluster client solely based on a zk client and a path
        val mycluster = CachePoolCluster.newZkCluster(zkPath, zookeeperClient)
        mycluster.ready() // give it sometime for the cluster to get the initial set of memberships

        val client = KetamaClientBuilder()
                .cachePoolCluster(mycluster)
                .build()
                .asInstanceOf[PartitionedClient]

        val count = 100
        (0 until count).foreach{
          n => {
            client.set("foo"+n, "bar"+n)()
          }
        }

        (0 until count).foreach {
          n => {
            val c = client.clientOf("foo"+n)
            c.get("foo"+n)().get.toString(CharsetUtil.UTF_8) mustEqual "bar"+n
          }
        }
      }

      "cache pool is changing" in {
        // create my cluster client solely based on a zk client and a path
        val mycluster = initializePool(5)

        val client = KetamaClientBuilder()
            .cachePoolCluster(mycluster)
            .build()
            .asInstanceOf[PartitionedClient]

        def trackCacheShards = {
          var trackedCacheShards = mutable.Set[Client]()
          val count = 100
          (0 until count).foreach {
            n => {
              val c = client.clientOf("foo"+n)
              trackedCacheShards += c
            }
          }
          trackedCacheShards
        }

        // initially there should be 5 cache shards being used
        trackCacheShards.size mustBe 5

        // add 2 more cache servers and update cache pool config data, now there should be 7 shards
        var additionalServers = List[EndpointStatus]()
        expectPoolStatus(mycluster, currentSize = 5, expectedPoolSize = 9, expectedAdd = 4, expectedRem = 0) {
          additionalServers = addMoreServers(4)
          updateCachePoolConfigData(9)
        }.get(10.seconds)() mustNot throwA[Exception]
        // Unlike CachePoolCluster, our KetamaClient doesn't have api to expose its internal state and
        // it shouldn't, hence here I don't really have a better way to wait for the client's key ring
        // redistribution to finish other than sleep for a while.
        Thread.sleep(1000)
        trackCacheShards.size mustBe 9

        // remove 2 cache servers and update cache pool config data, now there should be 7 shards
        expectPoolStatus(mycluster, currentSize = 9, expectedPoolSize = 7, expectedAdd = 0, expectedRem = 2) {
          additionalServers(0).update(Status.DEAD)
          additionalServers(1).update(Status.DEAD)
          updateCachePoolConfigData(7)
        }.get(10.seconds)() mustNot throwA[Exception]
        Thread.sleep(1000)
        trackCacheShards.size mustBe 7

        // remove 2 and add 4 more cache servers and update cache pool config data, now there should be 9 shards
        expectPoolStatus(mycluster, currentSize = 7, expectedPoolSize = 9, expectedAdd = 4, expectedRem = 2) {
          additionalServers(2).update(Status.DEAD)
          additionalServers(3).update(Status.DEAD)
          addMoreServers(4)
          updateCachePoolConfigData(9)
        }.get(10.seconds)() mustNot throwA[Exception]
        Thread.sleep(1000)
        trackCacheShards.size mustBe 9
      }
    }
    */

    def updateCachePoolConfigData(size: Int) {
      val cachePoolConfig: CachePoolConfig = new CachePoolConfig(cachePoolSize = size)
      var output: ByteArrayOutputStream = new ByteArrayOutputStream
      CachePoolConfig.jsonCodec.serialize(cachePoolConfig, output)
      zookeeperClient.get().setData(zkPath, output.toByteArray, -1)
    }

    // create temporary zk clients for additional cache servers since we will need to
    // de-register these services by expiring corresponding zk client session
    def addMoreServers(size: Int): List[EndpointStatus] = {
      (1 to size) map { _ =>
        val address: Option[InetSocketAddress] = ExternalMemcached.start()
        zkServerSetCluster.joinServerSet(address.get)
      } toList
    }

    def initializePool(
      expectedSize: Int,
      backupPool: Option[scala.collection.immutable.Set[CacheNode]]=None
    ): CachePoolCluster = {
      val myCachePool = new ZookeeperCachePoolCluster(zkPath, zookeeperClient, backupPool)

      myCachePool.ready() // wait until the pool is ready
      myCachePool.snap match {
        case (cachePool, changes) =>
          cachePool.size mustBe expectedSize
      }
      myCachePool
    }

    /**
     * return a future which will be complete only if the cache pool changed AND the changes
     * meet all expected conditions after executing operation passed in
     * @param currentSize expected current pool size
     * @param expectedPoolSize expected pool size after changes, use -1 to expect any size
     * @param expectedAdd expected number of add event happened, use -1 to expect any number
     * @param expectedRem expected number of rem event happened, use -1 to expect any number
     * @param ops operation to execute
     */
    def expectPoolStatus(
      myCachePool: CachePoolCluster,
      currentSize: Int,
      expectedPoolSize: Int,
      expectedAdd: Int,
      expectedRem: Int
    )(ops: => Unit): Future[Unit] = {
      var addSeen = 0
      var remSeen = 0
      var poolSeen = mutable.HashSet[CacheNode]()

      def expectMore(spoolChanges: Spool[Cluster.Change[CacheNode]]): Future[Unit] = {
        spoolChanges match {
          case change *:: tail =>
            change match {
              case Cluster.Add(node) =>
                addSeen += 1
                poolSeen.add(node)
              case Cluster.Rem(node) =>
                remSeen += 1
                poolSeen.remove(node)
            }
            if ((expectedAdd == -1 || addSeen == expectedAdd) &&
                (expectedRem == -1 || remSeen == expectedRem) &&
                (expectedPoolSize == -1 || poolSeen.size == expectedPoolSize)) Future.Done
            else tail flatMap expectMore
        }
      }

      myCachePool.snap match {
        case (cachePool, changes) =>
          cachePool.size mustBe currentSize
          poolSeen ++= cachePool
          val retFuture = changes flatMap expectMore
          ops // invoke the function now
          retFuture
      }
    }
  }
}
