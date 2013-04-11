package com.twitter.finagle.memcached.integration

import _root_.java.io.ByteArrayOutputStream
import _root_.java.lang.{Boolean => JBoolean}
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
import com.twitter.finagle.memcached.replication._
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import com.twitter.finagle.stats.SummarizingStatsReceiver
import com.twitter.finagle.zookeeper.ZookeeperServerSetCluster
import com.twitter.finagle.WriteException
import com.twitter.thrift.Status
import com.twitter.util.{Duration, Future, Return, Throw}
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
      ExternalMemcached.start() match {
        case Some(address) =>
          val service = ClientBuilder()
            .hosts(Seq(address))
            .codec(new Memcached(stats))
            .hostConnectionLimit(1)
            .build()
          client = Client(service)

        case _ =>
          skip("Cannot start memcached. Skipping...")
      }
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
          Thread.sleep(2000)
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
                .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
                .failureAccrualParams(Int.MaxValue, Duration.Top)
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
                .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
                .failureAccrualParams(Int.MaxValue, Duration.Top)
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
            .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
            .failureAccrualParams(Int.MaxValue, Duration.Top)
            .cachePoolCluster(mycluster)
            .build()
            .asInstanceOf[PartitionedClient]

        def trackCacheShards = mutable.Set.empty[Client] ++ ((0 until 100).map {
          n => client.clientOf("foo"+n)
        })

        // initially there should be 5 cache shards being used
        trackCacheShards.size mustBe 5

        // add 4 more cache servers and update cache pool config data, now there should be 7 shards
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

        // remove another 2 cache servers and update cache pool config data, now there should be 5 shards
        expectPoolStatus(mycluster, currentSize = 7, expectedPoolSize = 5, expectedAdd = 0, expectedRem = 2) {
          additionalServers(2).update(Status.DEAD)
          additionalServers(3).update(Status.DEAD)
          updateCachePoolConfigData(5)
        }.get(10.seconds)() mustNot throwA[Exception]
        Thread.sleep(1000)
        trackCacheShards.size mustBe 5

        // add 2 more cache servers and update cache pool config data, now there should be 7 shards
        expectPoolStatus(mycluster, currentSize = 5, expectedPoolSize = 7, expectedAdd = 2, expectedRem = 0) {
          additionalServers = addMoreServers(2)
          updateCachePoolConfigData(7)
        }.get(10.seconds)() mustNot throwA[Exception]
        Thread.sleep(1000)
        trackCacheShards.size mustBe 7

        // add another 2 more cache servers and update cache pool config data, now there should be 9 shards
        expectPoolStatus(mycluster, currentSize = 7, expectedPoolSize = 9, expectedAdd = 2, expectedRem = 0) {
          additionalServers = addMoreServers(2)
          updateCachePoolConfigData(9)
        }.get(10.seconds)() mustNot throwA[Exception]
        Thread.sleep(1000)
        trackCacheShards.size mustBe 9

        // remove 2 and add 2, now there should be still 9 shards
        expectPoolStatus(mycluster, currentSize = 9, expectedPoolSize = 9, expectedAdd = 2, expectedRem = 2) {
          additionalServers(0).update(Status.DEAD)
          additionalServers(1).update(Status.DEAD)
          addMoreServers(2)
          updateCachePoolConfigData(9)
        }.get(10.seconds)() mustNot throwA[Exception]
        Thread.sleep(1000)
        trackCacheShards.size mustBe 9
      }

      "unmanaged cache pool is changing" in {
        // create my cluster client solely based on a zk client and a path
        val mycluster = initializePool(5, ignoreConfigData = true)

        val client = KetamaClientBuilder()
            .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
            .failureAccrualParams(Int.MaxValue, Duration.Top)
            .cachePoolCluster(mycluster)
            .build()
            .asInstanceOf[PartitionedClient]

        def trackCacheShards = mutable.Set.empty[Client] ++ ((0 until 100).map {
          n => client.clientOf("foo"+n)
        })

        // initially there should be 5 cache shards being used
        trackCacheShards.size mustBe 5

        // add 4 more cache servers and update cache pool config data, now there should be 7 shards
        var additionalServers = List[EndpointStatus]()
        expectPoolStatus(mycluster, currentSize = 5, expectedPoolSize = 9, expectedAdd = 4, expectedRem = 0) {
          additionalServers = addMoreServers(4)
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
        }.get(10.seconds)() mustNot throwA[Exception]
        Thread.sleep(1000)
        trackCacheShards.size mustBe 7
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
      backupPool: Option[scala.collection.immutable.Set[CacheNode]]=None,
      ignoreConfigData: Boolean = false
    ): Cluster[CacheNode] = {
      val myCachePool =
        if (! ignoreConfigData) CachePoolCluster.newZkCluster(zkPath, zookeeperClient, backupPool = backupPool)
        else CachePoolCluster.newUnmanagedZkCluster(zkPath, zookeeperClient)

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
      myCachePool: Cluster[CacheNode],
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

  "Replication client" should {

    /**
     * Note: This integration test requires a real Memcached server to run.
     */
    var shutdownRegistry = new ShutdownRegistryImpl
    var firstPoolAddresses = List[Option[InetSocketAddress]]()
    var secondPoolAddresses = List[Option[InetSocketAddress]]()

    val firstPoolPath = "/cache/test/silly-cache-1"
    val secondPoolPath = "/cache/test/silly-cache-2"
    var zookeeperServer: ZooKeeperTestServer = null
    var zookeeperClient: ZooKeeperClient = null

    doBefore {
      // start zookeeper server and create zookeeper client
      zookeeperServer = new ZooKeeperTestServer(0, shutdownRegistry)
      zookeeperServer.startNetwork()

      // connect to zookeeper server
      zookeeperClient = zookeeperServer.createClient(ZooKeeperClient.digestCredentials("user","pass"))

      // start two memcached server and join the cluster
      val firstPoolCluster = new ZookeeperServerSetCluster(
        ServerSets.create(zookeeperClient, ZooKeeperUtils.EVERYONE_READ_CREATOR_ALL, firstPoolPath))
      (0 to 1) foreach { _ =>
        val address: Option[InetSocketAddress] = ExternalMemcached.start()
        if (address == None) skip("Cannot start memcached. Skipping...")
        firstPoolAddresses :+= address
        firstPoolCluster.join(address.get)
      }

      val secondPoolCluster = new ZookeeperServerSetCluster(
        ServerSets.create(zookeeperClient, ZooKeeperUtils.EVERYONE_READ_CREATOR_ALL, secondPoolPath))
      (0 to 1) foreach { _ =>
        val address: Option[InetSocketAddress] = ExternalMemcached.start()
        if (address == None) skip("Cannot start memcached. Skipping...")
        secondPoolAddresses :+= address
        secondPoolCluster.join(address.get)
      }

      // set cache pool config node data
      val cachePoolConfig: CachePoolConfig = new CachePoolConfig(cachePoolSize = 2)
      val output: ByteArrayOutputStream = new ByteArrayOutputStream
      CachePoolConfig.jsonCodec.serialize(cachePoolConfig, output)
      zookeeperClient.get().setData(firstPoolPath, output.toByteArray, -1)
      zookeeperClient.get().setData(secondPoolPath, output.toByteArray, -1)

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

    "base replication client" in {
      "set & getOne" in {
        // create my cluster client solely based on a zk client and a path
        val mycluster1 = CachePoolCluster.newZkCluster(firstPoolPath, zookeeperClient)
        mycluster1.ready() // give it sometime for the cluster to get the initial set of memberships
        val mycluster2 = CachePoolCluster.newZkCluster(secondPoolPath, zookeeperClient)
        mycluster2.ready() // give it sometime for the cluster to get the initial set of memberships

        val client1 = KetamaClientBuilder()
            .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
            .cachePoolCluster(mycluster1)
            .failureAccrualParams(Int.MaxValue, 0.seconds)
            .build()
        val client2 = KetamaClientBuilder()
            .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
            .cachePoolCluster(mycluster2)
            .failureAccrualParams(Int.MaxValue, 0.seconds)
            .build()
        val replicatedClient = new BaseReplicationClient(Seq(client1, client2))

        // consistent
        replicatedClient.getOne("foo")() mustEqual None
        replicatedClient.set("foo", "bar")() mustEqual ConsistentReplication(())
        replicatedClient.getOne("foo")() mustEqual Some(stringToChannelBuffer("bar"))

        // inconsistent data
        client2.set("client2-only", "test")() mustNot throwA[Exception]
        replicatedClient.getOne("client2-only")() mustEqual Some(stringToChannelBuffer("test"))

        // inconsistent replica state
        ExternalMemcached.stop(firstPoolAddresses(0))
        ExternalMemcached.stop(firstPoolAddresses(1))
        replicatedClient.set("foo", "baz")() must beLike { case InconsistentReplication(Seq(Throw(_), Return(()))) => true }
        replicatedClient.getOne("foo")() mustEqual Some(stringToChannelBuffer("baz"))

        // all failed
        ExternalMemcached.stop(secondPoolAddresses(0))
        ExternalMemcached.stop(secondPoolAddresses(1))
        replicatedClient.set("foo", "baz")() must beLike { case FailedReplication(Seq(Throw(_), Throw(_))) => true }
        replicatedClient.getOne("foo")() must throwA[WriteException]
      }

      "set & getAll" in {
        // create my cluster client solely based on a zk client and a path
        val mycluster1 = CachePoolCluster.newZkCluster(firstPoolPath, zookeeperClient)
        mycluster1.ready() // give it sometime for the cluster to get the initial set of memberships
        val mycluster2 = CachePoolCluster.newZkCluster(secondPoolPath, zookeeperClient)
        mycluster2.ready() // give it sometime for the cluster to get the initial set of memberships

        val client1 = KetamaClientBuilder()
            .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
            .cachePoolCluster(mycluster1)
            .failureAccrualParams(Int.MaxValue, 0.seconds)
            .build()
        val client2 = KetamaClientBuilder()
            .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
            .cachePoolCluster(mycluster2)
            .failureAccrualParams(Int.MaxValue, 0.seconds)
            .build()
        val replicatedClient = new BaseReplicationClient(Seq(client1, client2))

        // consistent
        replicatedClient.getAll("foo")() mustEqual ConsistentReplication(None)
        replicatedClient.set("foo", "bar")() mustEqual ConsistentReplication(())
        replicatedClient.getAll("foo")() mustEqual ConsistentReplication(Some(stringToChannelBuffer("bar")))

        // inconsistent data
        client2.set("client2-only", "test")() mustNot throwA[Exception]
        replicatedClient.getAll("client2-only")() mustEqual InconsistentReplication(Seq(Return(None), Return(Some(stringToChannelBuffer("test")))))

        // inconsistent replica state
        ExternalMemcached.stop(firstPoolAddresses(0))
        ExternalMemcached.stop(firstPoolAddresses(1))
        replicatedClient.set("foo", "baz")() must beLike { case InconsistentReplication(Seq(Throw(_), Return(()))) => true }
        replicatedClient.getAll("foo")() must beLike { case InconsistentReplication(Seq(Throw(_), Return(Some(v)))) => v equals stringToChannelBuffer("baz") }

        // all failed
        ExternalMemcached.stop(secondPoolAddresses(0))
        ExternalMemcached.stop(secondPoolAddresses(1))
        replicatedClient.set("foo", "baz")() must beLike { case FailedReplication(Seq(Throw(_), Throw(_))) => true }
        replicatedClient.getAll("foo")() must beLike { case FailedReplication(Seq(Throw(_), Throw(_))) => true }
      }

      "delete" in {
        // create my cluster client solely based on a zk client and a path
        val mycluster1 = CachePoolCluster.newZkCluster(firstPoolPath, zookeeperClient)
        mycluster1.ready() // give it sometime for the cluster to get the initial set of memberships
        val mycluster2 = CachePoolCluster.newZkCluster(secondPoolPath, zookeeperClient)
        mycluster2.ready() // give it sometime for the cluster to get the initial set of memberships

        val client1 = KetamaClientBuilder()
            .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
            .cachePoolCluster(mycluster1)
            .failureAccrualParams(Int.MaxValue, 0.seconds)
            .build()
        val client2 = KetamaClientBuilder()
            .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
            .cachePoolCluster(mycluster2)
            .failureAccrualParams(Int.MaxValue, 0.seconds)
            .build()
        val replicatedClient = new BaseReplicationClient(Seq(client1, client2))

        // consistent
        replicatedClient.delete("empty-key")() mustEqual ConsistentReplication(false)

        replicatedClient.set("foo", "bar")() mustEqual ConsistentReplication(())
        replicatedClient.getAll("foo")() mustEqual ConsistentReplication(Some(stringToChannelBuffer("bar")))
        replicatedClient.delete("foo")() mustEqual ConsistentReplication(true)

        // inconsistent data
        client2.add("client2-only", "bar")() mustEqual true
        replicatedClient.delete("client2-only")() must beLike { case InconsistentReplication(Seq(Return(JBoolean.FALSE), Return(JBoolean.TRUE))) => true }

        // inconsistent replica state
        client2.set("client2-only", "bar")() mustEqual ()
        ExternalMemcached.stop(firstPoolAddresses(0))
        ExternalMemcached.stop(firstPoolAddresses(1))
        replicatedClient.delete("client2-only")() must beLike { case InconsistentReplication(Seq(Throw(_), Return(JBoolean.TRUE))) => true }

        // all failed
        ExternalMemcached.stop(secondPoolAddresses(0))
        ExternalMemcached.stop(secondPoolAddresses(1))
        replicatedClient.delete("client2-only")() must beLike { case FailedReplication(Seq(Throw(_), Throw(_))) => true }
      }

      "getsAll & cas" in {
        // create my cluster client solely based on a zk client and a path
        val mycluster1 = CachePoolCluster.newZkCluster(firstPoolPath, zookeeperClient)
        mycluster1.ready() // give it sometime for the cluster to get the initial set of memberships
        val mycluster2 = CachePoolCluster.newZkCluster(secondPoolPath, zookeeperClient)
        mycluster2.ready() // give it sometime for the cluster to get the initial set of memberships

        val client1 = KetamaClientBuilder()
            .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
            .cachePoolCluster(mycluster1)
            .failureAccrualParams(Int.MaxValue, 0.seconds)
            .build()
        val client2 = KetamaClientBuilder()
            .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
            .cachePoolCluster(mycluster2)
            .failureAccrualParams(Int.MaxValue, 0.seconds)
            .build()
        val replicatedClient = new BaseReplicationClient(Seq(client1, client2))

        // consistent
        replicatedClient.set("foo", "bar")() mustEqual ConsistentReplication(())
        replicatedClient.getsAll("foo")() mustEqual ConsistentReplication(Some((stringToChannelBuffer("bar"), RCasUnique(Seq("1", "1")))))
        client1.set("foo", "bar")() mustEqual ()
        replicatedClient.getsAll("foo")() mustEqual ConsistentReplication(Some((stringToChannelBuffer("bar"), RCasUnique(Seq("2", "1")))))
        replicatedClient.cas("foo", "baz", Seq("2", "1"))() mustEqual ConsistentReplication(true)
        replicatedClient.cas("foo", "baz", Seq("3", "2"))() mustEqual ConsistentReplication(true)
        client1.set("foo", "bar")() mustEqual ()
        client2.set("foo", "bar")() mustEqual ()
        replicatedClient.cas("foo", "baz", Seq("4", "3"))() mustEqual ConsistentReplication(false)
        replicatedClient.delete("foo")() mustEqual ConsistentReplication(true)
        replicatedClient.getsAll("foo")() mustEqual ConsistentReplication(None)

        // inconsistent data
        client1.set("foo", "bar")() mustEqual ()
        client2.set("foo", "baz")() mustEqual ()
        replicatedClient.getsAll("foo")() mustEqual InconsistentReplication(Seq(Return(Some((stringToChannelBuffer("bar"), SCasUnique("6")))), Return(Some((stringToChannelBuffer("baz"), SCasUnique("5"))))))
        client1.delete("foo")() mustEqual true
        replicatedClient.getsAll("foo")() mustEqual InconsistentReplication(Seq(Return(None), Return(Some((stringToChannelBuffer("baz"), SCasUnique("5"))))))
        replicatedClient.cas("foo", "bar", Seq("7", "5"))() must beLike { case InconsistentReplication(Seq(Throw(_), Return(JBoolean.TRUE))) => true }
        client1.set("foo", "bar")() mustEqual ()
        replicatedClient.cas("foo", "bar", Seq("6", "6"))() mustEqual InconsistentReplication(Seq(Return(false), Return(true)))

        // inconsistent replica state
        ExternalMemcached.stop(firstPoolAddresses(0))
        ExternalMemcached.stop(firstPoolAddresses(1))
        replicatedClient.getsAll("foo")() must beLike { case InconsistentReplication(Seq(Throw(_), Return(Some((v, SCasUnique(_)))))) => v equals stringToChannelBuffer("bar") }
        replicatedClient.cas("foo", "bar", Seq("7", "7"))() must beLike { case InconsistentReplication(Seq(Throw(_), Return(JBoolean.TRUE))) => true }

        // all failed
        ExternalMemcached.stop(secondPoolAddresses(0))
        ExternalMemcached.stop(secondPoolAddresses(1))
        replicatedClient.getsAll("foo")() must beLike { case FailedReplication(Seq(Throw(_), Throw(_))) => true }
        replicatedClient.cas("foo", "bar", Seq("7", "7"))() must beLike { case FailedReplication(Seq(Throw(_), Throw(_))) => true }
      }

      "add & replace" in {
        // create my cluster client solely based on a zk client and a path
        val mycluster1 = CachePoolCluster.newZkCluster(firstPoolPath, zookeeperClient)
        mycluster1.ready() // give it sometime for the cluster to get the initial set of memberships
        val mycluster2 = CachePoolCluster.newZkCluster(secondPoolPath, zookeeperClient)
        mycluster2.ready() // give it sometime for the cluster to get the initial set of memberships

        val client1 = KetamaClientBuilder()
            .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
            .cachePoolCluster(mycluster1)
            .failureAccrualParams(Int.MaxValue, 0.seconds)
            .build()
        val client2 = KetamaClientBuilder()
            .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
            .cachePoolCluster(mycluster2)
            .failureAccrualParams(Int.MaxValue, 0.seconds)
            .build()
        val replicatedClient = new BaseReplicationClient(Seq(client1, client2))

        // consistent
        replicatedClient.add("foo", "bar")() mustEqual ConsistentReplication(true)
        replicatedClient.getAll("foo")() mustEqual ConsistentReplication(Some(stringToChannelBuffer("bar")))

        replicatedClient.replace("foo", "baz")() mustEqual ConsistentReplication(true)
        replicatedClient.getAll("foo")() mustEqual ConsistentReplication(Some(stringToChannelBuffer("baz")))

        replicatedClient.add("foo", "bar")() mustEqual ConsistentReplication(false)
        replicatedClient.replace("no-such-key", "test")() mustEqual ConsistentReplication(false)

        // inconsistent data
        client1.add("client1-only", "test")() mustEqual true
        client2.add("client2-only", "test")() mustEqual true
        replicatedClient.add("client2-only", "test")() must beLike { case InconsistentReplication(Seq(Return(JBoolean.TRUE), Return(JBoolean.FALSE))) => true }
        replicatedClient.replace("client1-only", "test")() must beLike { case InconsistentReplication(Seq(Return(JBoolean.TRUE), Return(JBoolean.FALSE))) => true }

        // inconsistent replica state
        ExternalMemcached.stop(firstPoolAddresses(0))
        ExternalMemcached.stop(firstPoolAddresses(1))
        replicatedClient.add("client2-only", "test")() must beLike { case InconsistentReplication(Seq(Throw(_), Return(JBoolean.FALSE))) => true }
        replicatedClient.replace("client1-only", "test")() must beLike { case InconsistentReplication(Seq(Throw(_), Return(JBoolean.FALSE))) => true }

        // all failed
        ExternalMemcached.stop(secondPoolAddresses(0))
        ExternalMemcached.stop(secondPoolAddresses(1))
        replicatedClient.add("client2-only", "test")() must beLike { case FailedReplication(Seq(Throw(_), Throw(_))) => true }
        replicatedClient.replace("client1-only", "test")() must beLike { case FailedReplication(Seq(Throw(_), Throw(_))) => true }
      }

      "incr & decr" in {
        // create my cluster client solely based on a zk client and a path
        val mycluster1 = CachePoolCluster.newZkCluster(firstPoolPath, zookeeperClient)
        mycluster1.ready() // give it sometime for the cluster to get the initial set of memberships
        val mycluster2 = CachePoolCluster.newZkCluster(secondPoolPath, zookeeperClient)
        mycluster2.ready() // give it sometime for the cluster to get the initial set of memberships

        val client1 = KetamaClientBuilder()
            .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
            .cachePoolCluster(mycluster1)
            .failureAccrualParams(Int.MaxValue, 0.seconds)
            .build()
        val client2 = KetamaClientBuilder()
            .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
            .cachePoolCluster(mycluster2)
            .failureAccrualParams(Int.MaxValue, 0.seconds)
            .build()
        val replicatedClient = new BaseReplicationClient(Seq(client1, client2))

        // consistent
        replicatedClient.set("foo", "1")() mustEqual ConsistentReplication(())
        replicatedClient.getAll("foo")() mustEqual ConsistentReplication(Some(stringToChannelBuffer("1")))
        replicatedClient.incr("foo", 2)() mustEqual ConsistentReplication(Some(3L))
        replicatedClient.getAll("foo")() mustEqual ConsistentReplication(Some(stringToChannelBuffer("3")))
        replicatedClient.decr("foo", 1)() mustEqual ConsistentReplication(Some(2L))
        replicatedClient.getAll("foo")() mustEqual ConsistentReplication(Some(stringToChannelBuffer("2")))

        // inconsistent data
        client1.incr("foo", 1)() mustEqual Some(3L)
        replicatedClient.incr("foo", 1)() mustEqual InconsistentReplication(Seq(Return(Some(4L)), Return(Some(3L))))
        client2.decr("foo", 1)() mustEqual Some(2L)
        replicatedClient.decr("foo", 1)() mustEqual InconsistentReplication(Seq(Return(Some(3L)), Return(Some(1L))))

        client1.delete("foo")() mustEqual true
        replicatedClient.incr("foo", 1)() mustEqual InconsistentReplication(Seq(Return(None), Return(Some(2L))))

        // inconsistent replica state
        ExternalMemcached.stop(firstPoolAddresses(0))
        ExternalMemcached.stop(firstPoolAddresses(1))
        replicatedClient.decr("foo", 1)() must beLike { case InconsistentReplication(Seq(Throw(_), Return(Some(v)))) => v equals 1L }

        // all failed
        ExternalMemcached.stop(secondPoolAddresses(0))
        ExternalMemcached.stop(secondPoolAddresses(1))
        replicatedClient.decr("foo", 1)() must beLike { case FailedReplication(Seq(Throw(_), Throw(_))) => true }

      }

      "many keys" in {
        // create my cluster client solely based on a zk client and a path
        val mycluster1 = CachePoolCluster.newZkCluster(firstPoolPath, zookeeperClient)
        mycluster1.ready() // give it sometime for the cluster to get the initial set of memberships
        val mycluster2 = CachePoolCluster.newZkCluster(secondPoolPath, zookeeperClient)
        mycluster2.ready() // give it sometime for the cluster to get the initial set of memberships

        val client1 = KetamaClientBuilder()
            .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
            .cachePoolCluster(mycluster1)
            .failureAccrualParams(Int.MaxValue, 0.seconds)
            .build()
        val client2 = KetamaClientBuilder()
            .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
            .cachePoolCluster(mycluster2)
            .failureAccrualParams(Int.MaxValue, 0.seconds)
            .build()
        val replicatedClient = new BaseReplicationClient(Seq(client1, client2))

        val count = 100
        (0 until count).foreach{
          n => {
            replicatedClient.set("foo"+n, "bar"+n)()
          }
        }

        (0 until count).foreach {
          n => {
            replicatedClient.getAll("foo"+n)() mustEqual ConsistentReplication(Some(stringToChannelBuffer("bar"+n)))
          }
        }

        // shutdown primary pool
        ExternalMemcached.stop(firstPoolAddresses(0))
        ExternalMemcached.stop(firstPoolAddresses(1))

        (0 until count).foreach {
          n => {
            replicatedClient.getAll("foo"+n)() must beLike { case InconsistentReplication(Seq(Throw(_), Return(Some(v)))) => v equals stringToChannelBuffer("bar"+n) }
          }
        }
      }

      "replica down" in {
        // create my cluster client solely based on a zk client and a path
        val mycluster1 = CachePoolCluster.newZkCluster(firstPoolPath, zookeeperClient)
        mycluster1.ready() // give it sometime for the cluster to get the initial set of memberships
        val mycluster2 = CachePoolCluster.newZkCluster(secondPoolPath, zookeeperClient)
        mycluster2.ready() // give it sometime for the cluster to get the initial set of memberships

        val client1 = KetamaClientBuilder()
            .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
            .cachePoolCluster(mycluster1)
            .failureAccrualParams(Int.MaxValue, 0.seconds)
            .build()
        val client2 = KetamaClientBuilder()
            .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
            .cachePoolCluster(mycluster2)
            .failureAccrualParams(Int.MaxValue, 0.seconds)
            .build()
        val replicatedClient = new BaseReplicationClient(Seq(client1, client2))

        replicatedClient.set("foo", "bar")() mustEqual ConsistentReplication(())
        replicatedClient.getAll("foo")() mustEqual ConsistentReplication(Some(stringToChannelBuffer("bar")))

        // primary pool down
        ExternalMemcached.stop(firstPoolAddresses(0))
        ExternalMemcached.stop(firstPoolAddresses(1))

        replicatedClient.getAll("foo")() must beLike { case InconsistentReplication(Seq(Throw(_), Return(Some(v)))) => v equals stringToChannelBuffer("bar") }
        replicatedClient.set("foo", "baz")() must beLike { case InconsistentReplication(Seq(Throw(_), Return(()))) => true }

        // bring back primary pool
        ExternalMemcached.start(firstPoolAddresses(0))
        ExternalMemcached.start(firstPoolAddresses(1))

        replicatedClient.getAll("foo")() must beLike { case InconsistentReplication(Seq(Return(None), Return(Some(v)))) => v equals stringToChannelBuffer("baz") }
        replicatedClient.set("foo", "baz")() mustEqual ConsistentReplication(())
      }

      "non supported operation" in {
        // create my cluster client solely based on a zk client and a path
        val mycluster1 = CachePoolCluster.newZkCluster(firstPoolPath, zookeeperClient)
        mycluster1.ready() // give it sometime for the cluster to get the initial set of memberships
        val mycluster2 = CachePoolCluster.newZkCluster(secondPoolPath, zookeeperClient)
        mycluster2.ready() // give it sometime for the cluster to get the initial set of memberships

        val client1 = KetamaClientBuilder()
            .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
            .cachePoolCluster(mycluster1)
            .failureAccrualParams(Int.MaxValue, 0.seconds)
            .build()
        val client2 = KetamaClientBuilder()
            .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
            .cachePoolCluster(mycluster2)
            .failureAccrualParams(Int.MaxValue, 0.seconds)
            .build()
        val replicatedClient = new BaseReplicationClient(Seq(client1, client2))

        replicatedClient.append("not-supported", "value")() must throwA[UnsupportedOperationException]
        replicatedClient.prepend("not-supported", "value")() must throwA[UnsupportedOperationException]

      }
    }

    "simple replication client" in {
      "get & set" in {
        // create my cluster client solely based on a zk client and a path
        val mycluster1 = CachePoolCluster.newZkCluster(firstPoolPath, zookeeperClient)
        mycluster1.ready() // give it sometime for the cluster to get the initial set of memberships
        val mycluster2 = CachePoolCluster.newZkCluster(secondPoolPath, zookeeperClient)
        mycluster2.ready() // give it sometime for the cluster to get the initial set of memberships

        val client1 = KetamaClientBuilder()
            .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
            .cachePoolCluster(mycluster1)
            .failureAccrualParams(Int.MaxValue, 0.seconds)
            .build()
        val client2 = KetamaClientBuilder()
            .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
            .cachePoolCluster(mycluster2)
            .failureAccrualParams(Int.MaxValue, 0.seconds)
            .build()
        val replicatedClient = new SimpleReplicationClient(Seq(client1, client2))

        // consistent
        replicatedClient.get("foo")() mustEqual None
        replicatedClient.set("foo", "bar")() mustNot throwA[Exception]
        replicatedClient.get("foo")() mustEqual Some(stringToChannelBuffer("bar"))
        client1.get("foo")() mustEqual Some(stringToChannelBuffer("bar"))
        client2.get("foo")() mustEqual Some(stringToChannelBuffer("bar"))

        // inconsistent data
        client2.set("client2-only", "test")() mustNot throwA[Exception]
        client1.get("client2-only")() mustEqual None
        client2.get("client2-only")() mustEqual Some(stringToChannelBuffer("test"))
        replicatedClient.get("client2-only")() mustEqual Some(stringToChannelBuffer("test"))

        // set overwrites existing data
        replicatedClient.set("client2-only", "test-again")() mustNot throwA[Exception]
        replicatedClient.get("client2-only")() mustEqual Some(stringToChannelBuffer("test-again"))
        client1.get("client2-only")() mustEqual Some(stringToChannelBuffer("test-again"))
        client1.get("client2-only")() mustEqual Some(stringToChannelBuffer("test-again"))

        // inconsistent replica state
        ExternalMemcached.stop(firstPoolAddresses(0))
        ExternalMemcached.stop(firstPoolAddresses(1))
        replicatedClient.set("foo", "baz")() must throwA[SimpleReplicationFailure]
        replicatedClient.get("foo")() mustNot throwA[Exception]

        ExternalMemcached.stop(secondPoolAddresses(0))
        ExternalMemcached.stop(secondPoolAddresses(1))
        replicatedClient.set("foo", "baz")() must throwA[SimpleReplicationFailure]
        replicatedClient.get("foo")() must throwA[WriteException]
      }

      "gets & cas" in {
        // create my cluster client solely based on a zk client and a path
        val mycluster1 = CachePoolCluster.newZkCluster(firstPoolPath, zookeeperClient)
        mycluster1.ready() // give it sometime for the cluster to get the initial set of memberships
        val mycluster2 = CachePoolCluster.newZkCluster(secondPoolPath, zookeeperClient)
        mycluster2.ready() // give it sometime for the cluster to get the initial set of memberships

        val client1 = KetamaClientBuilder()
            .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
            .cachePoolCluster(mycluster1)
            .failureAccrualParams(Int.MaxValue, 0.seconds)
            .build()
        val client2 = KetamaClientBuilder()
            .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
            .cachePoolCluster(mycluster2)
            .failureAccrualParams(Int.MaxValue, 0.seconds)
            .build()
        val replicatedClient = new SimpleReplicationClient(Seq(client1, client2))

        // consistent
        replicatedClient.gets("foo")() mustEqual None
        replicatedClient.set("foo", "bar")() mustNot throwA[Exception]
        replicatedClient.gets("foo")() mustEqual Some((stringToChannelBuffer("bar"), stringToChannelBuffer("1|1")))

        // inconsistent data
        client1.set("inconsistent-key", "client1")() mustNot throwA[Exception]
        client2.set("inconsistent-key", "client2")() mustNot throwA[Exception]
        replicatedClient.gets("inconsistent-key")() mustEqual None

        // cas overwrites existing data
        replicatedClient.cas("foo", "baz", stringToChannelBuffer("1|1"))() mustEqual true

        // inconsistent replica state
        ExternalMemcached.stop(firstPoolAddresses(0))
        ExternalMemcached.stop(firstPoolAddresses(1))
        replicatedClient.cas("foo", "baz", stringToChannelBuffer("2|3"))() must throwA[SimpleReplicationFailure]
        replicatedClient.gets("foo")() must throwA[SimpleReplicationFailure]
      }

      "delete" in {
        // create my cluster client solely based on a zk client and a path
        val mycluster1 = CachePoolCluster.newZkCluster(firstPoolPath, zookeeperClient)
        mycluster1.ready() // give it sometime for the cluster to get the initial set of memberships
        val mycluster2 = CachePoolCluster.newZkCluster(secondPoolPath, zookeeperClient)
        mycluster2.ready() // give it sometime for the cluster to get the initial set of memberships

        val client1 = KetamaClientBuilder()
            .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
            .cachePoolCluster(mycluster1)
            .failureAccrualParams(Int.MaxValue, 0.seconds)
            .build()
        val client2 = KetamaClientBuilder()
            .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
            .cachePoolCluster(mycluster2)
            .failureAccrualParams(Int.MaxValue, 0.seconds)
            .build()
        val replicatedClient = new SimpleReplicationClient(Seq(client1, client2))

        // consistent
        replicatedClient.delete("empty-key")() mustEqual false

        replicatedClient.set("foo", "bar")() mustNot throwA[Exception]
        replicatedClient.get("foo")() mustEqual Some(stringToChannelBuffer("bar"))
        client1.get("foo")() mustEqual Some(stringToChannelBuffer("bar"))
        client1.get("foo")() mustEqual Some(stringToChannelBuffer("bar"))
        replicatedClient.delete("foo")() mustEqual true
        client1.get("foo")() mustEqual None
        client2.get("foo")() mustEqual None

        // inconsistent data
        client2.add("client2-only", "bar")() mustEqual true
        client1.get("client2-only")() mustEqual None
        client2.get("client2-only")() mustEqual Some(stringToChannelBuffer("bar"))
        replicatedClient.delete("client2-only")() mustEqual false

        // inconsistent replica state
        client2.set("client2-only", "bar")() mustEqual ()
        client1.get("client2-only")() mustEqual None
        client2.get("client2-only")() mustEqual Some(stringToChannelBuffer("bar"))
        ExternalMemcached.stop(firstPoolAddresses(0))
        ExternalMemcached.stop(firstPoolAddresses(1))
        replicatedClient.delete("client2-only")() must throwA[SimpleReplicationFailure]
        ExternalMemcached.stop(secondPoolAddresses(0))
        ExternalMemcached.stop(secondPoolAddresses(1))
        replicatedClient.delete("client2-only")() must throwA[SimpleReplicationFailure]
      }

      "add & replace" in {
        // create my cluster client solely based on a zk client and a path
        val mycluster1 = CachePoolCluster.newZkCluster(firstPoolPath, zookeeperClient)
        mycluster1.ready() // give it sometime for the cluster to get the initial set of memberships
        val mycluster2 = CachePoolCluster.newZkCluster(secondPoolPath, zookeeperClient)
        mycluster2.ready() // give it sometime for the cluster to get the initial set of memberships

        val client1 = KetamaClientBuilder()
            .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
            .cachePoolCluster(mycluster1)
            .failureAccrualParams(Int.MaxValue, 0.seconds)
            .build()
        val client2 = KetamaClientBuilder()
            .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
            .cachePoolCluster(mycluster2)
            .failureAccrualParams(Int.MaxValue, 0.seconds)
            .build()
        val replicatedClient = new SimpleReplicationClient(Seq(client1, client2))

        // consistent
        replicatedClient.add("foo", "bar")() mustEqual true
        replicatedClient.get("foo")() mustEqual Some(stringToChannelBuffer("bar"))
        client1.get("foo")() mustEqual Some(stringToChannelBuffer("bar"))
        client2.get("foo")() mustEqual Some(stringToChannelBuffer("bar"))

        replicatedClient.replace("foo", "baz")() mustEqual true
        client1.get("foo")() mustEqual Some(stringToChannelBuffer("baz"))
        client2.get("foo")() mustEqual Some(stringToChannelBuffer("baz"))

        replicatedClient.add("foo", "bar")() mustEqual false

        replicatedClient.replace("no-such-key", "test")() mustEqual false

        // inconsistent data
        client1.add("client1-only", "test")() mustEqual true
        client2.add("client2-only", "test")() mustEqual true
        replicatedClient.add("client2-only", "test")() mustEqual false
        replicatedClient.replace("client1-only", "test")() mustEqual false

        // inconsistent replica state
        ExternalMemcached.stop(firstPoolAddresses(0))
        ExternalMemcached.stop(firstPoolAddresses(1))
        replicatedClient.add("client2-only", "test")() must throwA[SimpleReplicationFailure]
        replicatedClient.replace("client1-only", "test")() must throwA[SimpleReplicationFailure]
      }

      "indr & decr" in {
        // create my cluster client solely based on a zk client and a path
        val mycluster1 = CachePoolCluster.newZkCluster(firstPoolPath, zookeeperClient)
        mycluster1.ready() // give it sometime for the cluster to get the initial set of memberships
        val mycluster2 = CachePoolCluster.newZkCluster(secondPoolPath, zookeeperClient)
        mycluster2.ready() // give it sometime for the cluster to get the initial set of memberships

        val client1 = KetamaClientBuilder()
            .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
            .cachePoolCluster(mycluster1)
            .failureAccrualParams(Int.MaxValue, 0.seconds)
            .build()
        val client2 = KetamaClientBuilder()
            .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
            .cachePoolCluster(mycluster2)
            .failureAccrualParams(Int.MaxValue, 0.seconds)
            .build()
        val replicatedClient = new SimpleReplicationClient(Seq(client1, client2))

        // consistent
        replicatedClient.incr("foo", 2L)() mustEqual None
        replicatedClient.add("foo", "1")() mustEqual true
        replicatedClient.get("foo")() mustEqual Some(stringToChannelBuffer("1"))
        replicatedClient.incr("foo", 2L)() mustEqual Some(3L)
        replicatedClient.decr("foo", 1L)() mustEqual Some(2L)

        // inconsistent data
        client2.incr("foo", 1L)() mustEqual Some(3L)
        replicatedClient.incr("foo", 2L)() mustEqual None
        replicatedClient.decr("foo", 2L)() mustEqual None

        // inconsistent replica state
        ExternalMemcached.stop(firstPoolAddresses(0))
        ExternalMemcached.stop(firstPoolAddresses(1))
        replicatedClient.incr("foo", 2L)() must throwA[SimpleReplicationFailure]
        replicatedClient.decr("foo", 2L)() must throwA[SimpleReplicationFailure]
      }

      "many keys" in {
        // create my cluster client solely based on a zk client and a path
        val mycluster1 = CachePoolCluster.newZkCluster(firstPoolPath, zookeeperClient)
        mycluster1.ready() // give it sometime for the cluster to get the initial set of memberships
        val mycluster2 = CachePoolCluster.newZkCluster(secondPoolPath, zookeeperClient)
        mycluster2.ready() // give it sometime for the cluster to get the initial set of memberships

        val client1 = KetamaClientBuilder()
            .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
            .cachePoolCluster(mycluster1)
            .failureAccrualParams(Int.MaxValue, 0.seconds)
            .build()
        val client2 = KetamaClientBuilder()
            .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
            .cachePoolCluster(mycluster2)
            .failureAccrualParams(Int.MaxValue, 0.seconds)
            .build()
        val replicatedClient = new SimpleReplicationClient(Seq(client1, client2))

        val count = 100
        (0 until count).foreach{
          n => {
            replicatedClient.set("foo"+n, "bar"+n)()
          }
        }

        (0 until count).foreach {
          n => {
            replicatedClient.get("foo"+n)() mustEqual Some(stringToChannelBuffer("bar"+n))
            client1.get("foo"+n)() mustEqual Some(stringToChannelBuffer("bar"+n))
            client2.get("foo"+n)() mustEqual Some(stringToChannelBuffer("bar"+n))
          }
        }

        // shutdown primary pool
        ExternalMemcached.stop(firstPoolAddresses(0))
        ExternalMemcached.stop(firstPoolAddresses(1))

        (0 until count).foreach {
          n => {
            replicatedClient.get("foo"+n)() mustEqual Some(stringToChannelBuffer("bar"+n))
          }
        }
      }

      "replica down" in {
        // create my cluster client solely based on a zk client and a path
        val mycluster1 = CachePoolCluster.newZkCluster(firstPoolPath, zookeeperClient)
        mycluster1.ready() // give it sometime for the cluster to get the initial set of memberships
        val mycluster2 = CachePoolCluster.newZkCluster(secondPoolPath, zookeeperClient)
        mycluster2.ready() // give it sometime for the cluster to get the initial set of memberships

        val client1 = KetamaClientBuilder()
            .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
            .cachePoolCluster(mycluster1)
            .failureAccrualParams(Int.MaxValue, 0.seconds)
            .build()
        val client2 = KetamaClientBuilder()
            .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
            .cachePoolCluster(mycluster2)
            .failureAccrualParams(Int.MaxValue, 0.seconds)
            .build()
        val replicatedClient = new SimpleReplicationClient(Seq(client1, client2))

        replicatedClient.set("foo", "bar")()
        replicatedClient.get("foo")() mustEqual Some(stringToChannelBuffer("bar"))
        client1.get("foo")() mustEqual Some(stringToChannelBuffer("bar"))
        client2.get("foo")() mustEqual Some(stringToChannelBuffer("bar"))

        // primary pool down
        ExternalMemcached.stop(firstPoolAddresses(0))
        ExternalMemcached.stop(firstPoolAddresses(1))

        replicatedClient.get("foo")() mustEqual Some(stringToChannelBuffer("bar"))
        replicatedClient.set("foo", "baz")() must throwA[SimpleReplicationFailure] // set failed

        // bring back primary pool
        ExternalMemcached.start(firstPoolAddresses(0))
        ExternalMemcached.start(firstPoolAddresses(1))

        replicatedClient.get("foo")() mustEqual Some(stringToChannelBuffer("baz"))
        client1.get("foo")() mustEqual None
        client2.get("foo")() mustEqual Some(stringToChannelBuffer("baz"))
        replicatedClient.set("foo", "baz")() mustNot throwA[Exception]
      }

      "non supported operation" in {
        // create my cluster client solely based on a zk client and a path
        val mycluster1 = CachePoolCluster.newZkCluster(firstPoolPath, zookeeperClient)
        mycluster1.ready() // give it sometime for the cluster to get the initial set of memberships
        val mycluster2 = CachePoolCluster.newZkCluster(secondPoolPath, zookeeperClient)
        mycluster2.ready() // give it sometime for the cluster to get the initial set of memberships

        val client1 = KetamaClientBuilder()
            .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
            .cachePoolCluster(mycluster1)
            .failureAccrualParams(Int.MaxValue, 0.seconds)
            .build()
        val client2 = KetamaClientBuilder()
            .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
            .cachePoolCluster(mycluster2)
            .failureAccrualParams(Int.MaxValue, 0.seconds)
            .build()
        val replicatedClient = new SimpleReplicationClient(Seq(client1, client2))

        replicatedClient.append("not-supported", "value")() must throwA[UnsupportedOperationException]
        replicatedClient.prepend("not-supported", "value")() must throwA[UnsupportedOperationException]
      }
    }
  }
}
