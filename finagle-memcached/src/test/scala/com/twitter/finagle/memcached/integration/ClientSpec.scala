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
import com.twitter.finagle.{MemcachedClient, WriteException}
import com.twitter.thrift.Status
import com.twitter.util.{Await, Duration, Future, Return, Throw}
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
        Await.result(client.delete("foo"))
        Await.result(client.get("foo")) mustEqual None
        Await.result(client.set("foo", "bar"))
        Await.result(client.get("foo")).get.toString(CharsetUtil.UTF_8) mustEqual "bar"
      }

      "get" in {
        Await.result(client.set("foo", "bar"))
        Await.result(client.set("baz", "boing"))
        val result = Await.result(client.get(Seq("foo", "baz", "notthere")))
          .map { case (key, value) => (key, value.toString(CharsetUtil.UTF_8)) }
        result mustEqual Map(
          "foo" -> "bar",
          "baz" -> "boing"
        )
      }

      "gets" in {
        Await.result(client.set("foos", "xyz"))
        Await.result(client.set("bazs", "xyz"))
        Await.result(client.set("bazs", "zyx"))
        val result = Await.result(client.gets(Seq("foos", "bazs", "somethingelse")))
          .map { case (key, (value, casUnique)) =>
            (key, (value.toString(CharsetUtil.UTF_8), casUnique.toString(CharsetUtil.UTF_8)))
          }

        result mustEqual Map(
          "foos" -> ("xyz", "1"),  // the "cas unique" values are predictable from a fresh memcached
          "bazs" -> ("zyx", "3")
        )
      }

      "cas" in {
        Await.result(client.set("x", "y"))
        val Some((value, casUnique)) = Await.result(client.gets("x"))
        value.toString(CharsetUtil.UTF_8) must be_==("y")
        casUnique.toString(CharsetUtil.UTF_8) must be_==("1")

        Await.result(client.cas("x", "z", "2")) must beFalse
        Await.result(client.cas("x", "z", casUnique)) must beTrue
        val res = Await.result(client.get("x"))
        res must beSomething
        res.get.toString(CharsetUtil.UTF_8) must be_==("z")
      }

      "append & prepend" in {
        Await.result(client.set("foo", "bar"))
        Await.result(client.append("foo", "rab"))
        Await.result(client.get("foo")).get.toString(CharsetUtil.UTF_8) mustEqual "barrab"
        Await.result(client.prepend("foo", "rab"))
        Await.result(client.get("foo")).get.toString(CharsetUtil.UTF_8) mustEqual "rabbarrab"
      }

      "incr & decr" in {
        // As of memcached 1.4.8 (issue 221), empty values are no longer treated as integers
        Await.result(client.set("foo", "0"))
        Await.result(client.incr("foo"))    mustEqual Some(1L)
        Await.result(client.incr("foo", 2)) mustEqual Some(3L)
        Await.result(client.decr("foo"))    mustEqual Some(2L)

        Await.result(client.set("foo", "0"))
        Await.result(client.incr("foo"))    mustEqual Some(1L)
        val l = 1L << 50
        Await.result(client.incr("foo", l)) mustEqual Some(l + 1L)
        Await.result(client.decr("foo"))    mustEqual Some(l)
        Await.result(client.decr("foo", l)) mustEqual Some(0L)
      }

      "stats" in {
        val stats = Await.result(client.stats())
        stats must notBeEmpty
        stats.foreach { stat =>
          stat must startWith("STAT")
        }
      }

      "send malformed keys" in {
        // test key validation trait
        Await.result(client.get("fo o")) must throwA[ClientError]
        Await.result(client.set("", "bar")) must throwA[ClientError]
        Await.result(client.get("    foo")) must throwA[ClientError]
        Await.result(client.get("foo   ")) must throwA[ClientError]
        Await.result(client.get("    foo")) must throwA[ClientError]
        val nullString: String = null
        Await.result(client.get(nullString)) must throwA[ClientError]
        Await.result(client.set(nullString, "bar")) must throwA[ClientError]
        Await.result(client.set("    ", "bar")) must throwA[ClientError]
        Await.result(client.set("\t", "bar")) mustNot throwA[ClientError] // \t is allowed
        Await.result(client.set("\r", "bar")) must throwA[ClientError]
        Await.result(client.set("\n", "bar")) must throwA[ClientError]
        Await.result(client.set("\0", "bar")) must throwA[ClientError]

        val veryLongKey = "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz"
        Await.result(client.get(veryLongKey)) must throwA[ClientError]
        Await.ready(client.set(veryLongKey, "bar")).poll.get.isThrow mustBe true

        // test other keyed command validation
        val nullSeq:Seq[String] = null
        Await.result(client.get(nullSeq)) must throwA[ClientError]
        Await.result(client.gets(nullSeq)) must throwA[ClientError]
        Await.result(client.gets(Seq(null))) must throwA[ClientError]
        Await.result(client.gets(Seq(""))) must throwA[ClientError]
        Await.result(client.gets(Seq("foos", "bad key", "somethingelse"))) must throwA[ClientError]
        Await.result(client.append("bad key", "rab")) must throwA[ClientError]
        Await.result(client.prepend("bad key", "rab")) must throwA[ClientError]
        Await.result(client.replace("bad key", "bar")) must throwA[ClientError]
        Await.result(client.add("bad key", "2")) must throwA[ClientError]
        Await.result(client.cas("bad key", "z", "2")) must throwA[ClientError]
        Await.result(client.incr("bad key")) must throwA[ClientError]
        Await.result(client.decr("bad key")) must throwA[ClientError]
        Await.result(client.delete("bad key")) must throwA[ClientError]
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

        Await.result(client.delete("foo"))
        Await.result(client.get("foo")) mustEqual None
        Await.result(client.set("foo", "bar"))
        Await.result(client.get("foo")).get.toString(CharsetUtil.UTF_8) mustEqual "bar"
      }

      "even in future pool" in {
        lazy val client = KetamaClientBuilder()
            .nodes("localhost:%d,localhost:%d".format(address1.getPort, address2.getPort))
            .build()

        val futureResult = Future.value(true) flatMap {
          _ => client.get("foo")
        }

        Await.result(futureResult) mustEqual None
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
        Await.result(mycluster.ready) // give it sometime for the cluster to get the initial set of memberships
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
                if (Await.result(c.get("foo"+n))!=None) {
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

      Await.result(myCachePool.ready) // wait until the pool is ready
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
        Await.result(mycluster1.ready) // give it sometime for the cluster to get the initial set of memberships
        val mycluster2 = CachePoolCluster.newZkCluster(secondPoolPath, zookeeperClient)
        Await.result(mycluster2.ready) // give it sometime for the cluster to get the initial set of memberships

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
        Await.result(replicatedClient.getOne("foo")) mustEqual None
        Await.result(replicatedClient.set("foo", "bar")) mustEqual ConsistentReplication(())
        Await.result(replicatedClient.getOne("foo")) mustEqual Some(stringToChannelBuffer("bar"))

        // inconsistent data
        Await.result(client2.set("client2-only", "test")) mustNot throwA[Exception]
        Await.result(replicatedClient.getOne("client2-only")) mustEqual Some(stringToChannelBuffer("test"))

        // inconsistent replica state
        ExternalMemcached.stop(firstPoolAddresses(0))
        ExternalMemcached.stop(firstPoolAddresses(1))
        Await.result(replicatedClient.set("foo", "baz")) must beLike { case InconsistentReplication(Seq(Throw(_), Return(()))) => true }
        Await.result(replicatedClient.getOne("foo")) mustEqual Some(stringToChannelBuffer("baz"))

        // all failed
        ExternalMemcached.stop(secondPoolAddresses(0))
        ExternalMemcached.stop(secondPoolAddresses(1))
        Await.result(replicatedClient.set("foo", "baz")) must beLike { case FailedReplication(Seq(Throw(_), Throw(_))) => true }
        Await.result(replicatedClient.getOne("foo")) must throwA[WriteException]
      }

      "set & getAll" in {
        // create my cluster client solely based on a zk client and a path
        val mycluster1 = CachePoolCluster.newZkCluster(firstPoolPath, zookeeperClient)
        Await.result(mycluster1.ready) // give it sometime for the cluster to get the initial set of memberships
        val mycluster2 = CachePoolCluster.newZkCluster(secondPoolPath, zookeeperClient)
        Await.result(mycluster2.ready) // give it sometime for the cluster to get the initial set of memberships

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
        Await.result(replicatedClient.getAll("foo")) mustEqual ConsistentReplication(None)
        Await.result(replicatedClient.set("foo", "bar")) mustEqual ConsistentReplication(())
        Await.result(replicatedClient.getAll("foo")) mustEqual ConsistentReplication(Some(stringToChannelBuffer("bar")))

        // inconsistent data
        Await.result(client2.set("client2-only", "test")) mustNot throwA[Exception]
        Await.result(replicatedClient.getAll("client2-only")) mustEqual InconsistentReplication(Seq(Return(None), Return(Some(stringToChannelBuffer("test")))))

        // inconsistent replica state
        ExternalMemcached.stop(firstPoolAddresses(0))
        ExternalMemcached.stop(firstPoolAddresses(1))
        Await.result(replicatedClient.set("foo", "baz")) must beLike { case InconsistentReplication(Seq(Throw(_), Return(()))) => true }
        Await.result(replicatedClient.getAll("foo")) must beLike { case InconsistentReplication(Seq(Throw(_), Return(Some(v)))) => v equals stringToChannelBuffer("baz") }

        // all failed
        ExternalMemcached.stop(secondPoolAddresses(0))
        ExternalMemcached.stop(secondPoolAddresses(1))
        Await.result(replicatedClient.set("foo", "baz")) must beLike { case FailedReplication(Seq(Throw(_), Throw(_))) => true }
        Await.result(replicatedClient.getAll("foo")) must beLike { case FailedReplication(Seq(Throw(_), Throw(_))) => true }
      }

      "delete" in {
        // create my cluster client solely based on a zk client and a path
        val mycluster1 = CachePoolCluster.newZkCluster(firstPoolPath, zookeeperClient)
        Await.result(mycluster1.ready) // give it sometime for the cluster to get the initial set of memberships
        val mycluster2 = CachePoolCluster.newZkCluster(secondPoolPath, zookeeperClient)
        Await.result(mycluster2.ready) // give it sometime for the cluster to get the initial set of memberships

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
        Await.result(replicatedClient.delete("empty-key")) mustEqual ConsistentReplication(false)

        Await.result(replicatedClient.set("foo", "bar")) mustEqual ConsistentReplication(())
        Await.result(replicatedClient.getAll("foo")) mustEqual ConsistentReplication(Some(stringToChannelBuffer("bar")))
        Await.result(replicatedClient.delete("foo")) mustEqual ConsistentReplication(true)

        // inconsistent data
        Await.result(client2.add("client2-only", "bar")) mustEqual true
        Await.result(replicatedClient.delete("client2-only")) must beLike { case InconsistentReplication(Seq(Return(JBoolean.FALSE), Return(JBoolean.TRUE))) => true }

        // inconsistent replica state
        Await.result(client2.set("client2-only", "bar")) mustEqual ()
        ExternalMemcached.stop(firstPoolAddresses(0))
        ExternalMemcached.stop(firstPoolAddresses(1))
        Await.result(replicatedClient.delete("client2-only")) must beLike { case InconsistentReplication(Seq(Throw(_), Return(JBoolean.TRUE))) => true }

        // all failed
        ExternalMemcached.stop(secondPoolAddresses(0))
        ExternalMemcached.stop(secondPoolAddresses(1))
        Await.result(replicatedClient.delete("client2-only")) must beLike { case FailedReplication(Seq(Throw(_), Throw(_))) => true }
      }

      "getsAll & cas" in {
        // create my cluster client solely based on a zk client and a path
        val mycluster1 = CachePoolCluster.newZkCluster(firstPoolPath, zookeeperClient)
        Await.result(mycluster1.ready) // give it sometime for the cluster to get the initial set of memberships
        val mycluster2 = CachePoolCluster.newZkCluster(secondPoolPath, zookeeperClient)
        Await.result(mycluster2.ready) // give it sometime for the cluster to get the initial set of memberships

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
        Await.result(replicatedClient.set("foo", "bar")) mustEqual ConsistentReplication(())
        Await.result(replicatedClient.getsAll("foo")) mustEqual ConsistentReplication(Some((stringToChannelBuffer("bar"), RCasUnique(Seq("1", "1")))))
        Await.result(client1.set("foo", "bar")) mustEqual ()
        Await.result(replicatedClient.getsAll("foo")) mustEqual ConsistentReplication(Some((stringToChannelBuffer("bar"), RCasUnique(Seq("2", "1")))))
        Await.result(replicatedClient.cas("foo", "baz", Seq("2", "1"))) mustEqual ConsistentReplication(true)
        Await.result(replicatedClient.cas("foo", "baz", Seq("3", "2"))) mustEqual ConsistentReplication(true)
        Await.result(client1.set("foo", "bar")) mustEqual ()
        Await.result(client2.set("foo", "bar")) mustEqual ()
        Await.result(replicatedClient.cas("foo", "baz", Seq("4", "3"))) mustEqual ConsistentReplication(false)
        Await.result(replicatedClient.delete("foo")) mustEqual ConsistentReplication(true)
        Await.result(replicatedClient.getsAll("foo")) mustEqual ConsistentReplication(None)

        // inconsistent data
        Await.result(client1.set("foo", "bar")) mustEqual ()
        Await.result(client2.set("foo", "baz")) mustEqual ()
        Await.result(replicatedClient.getsAll("foo")) mustEqual InconsistentReplication(Seq(Return(Some((stringToChannelBuffer("bar"), SCasUnique("6")))), Return(Some((stringToChannelBuffer("baz"), SCasUnique("5"))))))
        Await.result(client1.delete("foo")) mustEqual true
        Await.result(replicatedClient.getsAll("foo")) mustEqual InconsistentReplication(Seq(Return(None), Return(Some((stringToChannelBuffer("baz"), SCasUnique("5"))))))
        Await.result(replicatedClient.cas("foo", "bar", Seq("7", "5"))) must beLike { case InconsistentReplication(Seq(Throw(_), Return(JBoolean.TRUE))) => true }
        Await.result(client1.set("foo", "bar")) mustEqual ()
        Await.result(replicatedClient.cas("foo", "bar", Seq("6", "6"))) mustEqual InconsistentReplication(Seq(Return(false), Return(true)))

        // inconsistent replica state
        ExternalMemcached.stop(firstPoolAddresses(0))
        ExternalMemcached.stop(firstPoolAddresses(1))
        Await.result(replicatedClient.getsAll("foo")) must beLike { case InconsistentReplication(Seq(Throw(_), Return(Some((v, SCasUnique(_)))))) => v equals stringToChannelBuffer("bar") }
        Await.result(replicatedClient.cas("foo", "bar", Seq("7", "7"))) must beLike { case InconsistentReplication(Seq(Throw(_), Return(JBoolean.TRUE))) => true }

        // all failed
        ExternalMemcached.stop(secondPoolAddresses(0))
        ExternalMemcached.stop(secondPoolAddresses(1))
        Await.result(replicatedClient.getsAll("foo")) must beLike { case FailedReplication(Seq(Throw(_), Throw(_))) => true }
        Await.result(replicatedClient.cas("foo", "bar", Seq("7", "7"))) must beLike { case FailedReplication(Seq(Throw(_), Throw(_))) => true }
      }

      "add & replace" in {
        // create my cluster client solely based on a zk client and a path
        val mycluster1 = CachePoolCluster.newZkCluster(firstPoolPath, zookeeperClient)
        Await.result(mycluster1.ready) // give it sometime for the cluster to get the initial set of memberships
        val mycluster2 = CachePoolCluster.newZkCluster(secondPoolPath, zookeeperClient)
        Await.result(mycluster2.ready) // give it sometime for the cluster to get the initial set of memberships

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
        Await.result(replicatedClient.add("foo", "bar")) mustEqual ConsistentReplication(true)
        Await.result(replicatedClient.getAll("foo")) mustEqual ConsistentReplication(Some(stringToChannelBuffer("bar")))

        Await.result(replicatedClient.replace("foo", "baz")) mustEqual ConsistentReplication(true)
        Await.result(replicatedClient.getAll("foo")) mustEqual ConsistentReplication(Some(stringToChannelBuffer("baz")))

        Await.result(replicatedClient.add("foo", "bar")) mustEqual ConsistentReplication(false)
        Await.result(replicatedClient.replace("no-such-key", "test")) mustEqual ConsistentReplication(false)

        // inconsistent data
        Await.result(client1.add("client1-only", "test")) mustEqual true
        Await.result(client2.add("client2-only", "test")) mustEqual true
        Await.result(replicatedClient.add("client2-only", "test")) must beLike { case InconsistentReplication(Seq(Return(JBoolean.TRUE), Return(JBoolean.FALSE))) => true }
        Await.result(replicatedClient.replace("client1-only", "test")) must beLike { case InconsistentReplication(Seq(Return(JBoolean.TRUE), Return(JBoolean.FALSE))) => true }

        // inconsistent replica state
        ExternalMemcached.stop(firstPoolAddresses(0))
        ExternalMemcached.stop(firstPoolAddresses(1))
        Await.result(replicatedClient.add("client2-only", "test")) must beLike { case InconsistentReplication(Seq(Throw(_), Return(JBoolean.FALSE))) => true }
        Await.result(replicatedClient.replace("client1-only", "test")) must beLike { case InconsistentReplication(Seq(Throw(_), Return(JBoolean.FALSE))) => true }

        // all failed
        ExternalMemcached.stop(secondPoolAddresses(0))
        ExternalMemcached.stop(secondPoolAddresses(1))
        Await.result(replicatedClient.add("client2-only", "test")) must beLike { case FailedReplication(Seq(Throw(_), Throw(_))) => true }
        Await.result(replicatedClient.replace("client1-only", "test")) must beLike { case FailedReplication(Seq(Throw(_), Throw(_))) => true }
      }

      "incr & decr" in {
        // create my cluster client solely based on a zk client and a path
        val mycluster1 = CachePoolCluster.newZkCluster(firstPoolPath, zookeeperClient)
        Await.result(mycluster1.ready) // give it sometime for the cluster to get the initial set of memberships
        val mycluster2 = CachePoolCluster.newZkCluster(secondPoolPath, zookeeperClient)
        Await.result(mycluster2.ready) // give it sometime for the cluster to get the initial set of memberships

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
        Await.result(replicatedClient.set("foo", "1")) mustEqual ConsistentReplication(())
        Await.result(replicatedClient.getAll("foo")) mustEqual ConsistentReplication(Some(stringToChannelBuffer("1")))
        Await.result(replicatedClient.incr("foo", 2)) mustEqual ConsistentReplication(Some(3L))
        Await.result(replicatedClient.getAll("foo")) mustEqual ConsistentReplication(Some(stringToChannelBuffer("3")))
        Await.result(replicatedClient.decr("foo", 1)) mustEqual ConsistentReplication(Some(2L))
        Await.result(replicatedClient.getAll("foo")) mustEqual ConsistentReplication(Some(stringToChannelBuffer("2")))

        // inconsistent data
        Await.result(client1.incr("foo", 1)) mustEqual Some(3L)
        Await.result(replicatedClient.incr("foo", 1)) mustEqual InconsistentReplication(Seq(Return(Some(4L)), Return(Some(3L))))
        Await.result(client2.decr("foo", 1)) mustEqual Some(2L)
        Await.result(replicatedClient.decr("foo", 1)) mustEqual InconsistentReplication(Seq(Return(Some(3L)), Return(Some(1L))))

        Await.result(client1.delete("foo")) mustEqual true
        Await.result(replicatedClient.incr("foo", 1)) mustEqual InconsistentReplication(Seq(Return(None), Return(Some(2L))))

        // inconsistent replica state
        ExternalMemcached.stop(firstPoolAddresses(0))
        ExternalMemcached.stop(firstPoolAddresses(1))
        Await.result(replicatedClient.decr("foo", 1)) must beLike { case InconsistentReplication(Seq(Throw(_), Return(Some(v)))) => v equals 1L }

        // all failed
        ExternalMemcached.stop(secondPoolAddresses(0))
        ExternalMemcached.stop(secondPoolAddresses(1))
        Await.result(replicatedClient.decr("foo", 1)) must beLike { case FailedReplication(Seq(Throw(_), Throw(_))) => true }

      }

      "many keys" in {
        // create my cluster client solely based on a zk client and a path
        val mycluster1 = CachePoolCluster.newZkCluster(firstPoolPath, zookeeperClient)
        Await.result(mycluster1.ready) // give it sometime for the cluster to get the initial set of memberships
        val mycluster2 = CachePoolCluster.newZkCluster(secondPoolPath, zookeeperClient)
        Await.result(mycluster2.ready) // give it sometime for the cluster to get the initial set of memberships

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
            Await.result(replicatedClient.set("foo"+n, "bar"+n))
          }
        }

        (0 until count).foreach {
          n => {
            Await.result(replicatedClient.getAll("foo"+n)) mustEqual ConsistentReplication(Some(stringToChannelBuffer("bar"+n)))
          }
        }

        // shutdown primary pool
        ExternalMemcached.stop(firstPoolAddresses(0))
        ExternalMemcached.stop(firstPoolAddresses(1))

        (0 until count).foreach {
          n => {
            Await.result(replicatedClient.getAll("foo"+n)) must beLike { case InconsistentReplication(Seq(Throw(_), Return(Some(v)))) => v equals stringToChannelBuffer("bar"+n) }
          }
        }
      }

      "replica down" in {
        // create my cluster client solely based on a zk client and a path
        val mycluster1 = CachePoolCluster.newZkCluster(firstPoolPath, zookeeperClient)
        Await.result(mycluster1.ready) // give it sometime for the cluster to get the initial set of memberships
        val mycluster2 = CachePoolCluster.newZkCluster(secondPoolPath, zookeeperClient)
        Await.result(mycluster2.ready) // give it sometime for the cluster to get the initial set of memberships

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

        Await.result(replicatedClient.set("foo", "bar")) mustEqual ConsistentReplication(())
        Await.result(replicatedClient.getAll("foo")) mustEqual ConsistentReplication(Some(stringToChannelBuffer("bar")))

        // primary pool down
        ExternalMemcached.stop(firstPoolAddresses(0))
        ExternalMemcached.stop(firstPoolAddresses(1))

        Await.result(replicatedClient.getAll("foo")) must beLike { case InconsistentReplication(Seq(Throw(_), Return(Some(v)))) => v equals stringToChannelBuffer("bar") }
        Await.result(replicatedClient.set("foo", "baz")) must beLike { case InconsistentReplication(Seq(Throw(_), Return(()))) => true }

        // bring back primary pool
        ExternalMemcached.start(firstPoolAddresses(0))
        ExternalMemcached.start(firstPoolAddresses(1))

        Await.result(replicatedClient.getAll("foo")) must beLike { case InconsistentReplication(Seq(Return(None), Return(Some(v)))) => v equals stringToChannelBuffer("baz") }
        Await.result(replicatedClient.set("foo", "baz")) mustEqual ConsistentReplication(())
      }

      "non supported operation" in {
        // create my cluster client solely based on a zk client and a path
        val mycluster1 = CachePoolCluster.newZkCluster(firstPoolPath, zookeeperClient)
        Await.result(mycluster1.ready) // give it sometime for the cluster to get the initial set of memberships
        val mycluster2 = CachePoolCluster.newZkCluster(secondPoolPath, zookeeperClient)
        Await.result(mycluster2.ready) // give it sometime for the cluster to get the initial set of memberships

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

        Await.result(replicatedClient.append("not-supported", "value")) must throwA[UnsupportedOperationException]
        Await.result(replicatedClient.prepend("not-supported", "value")) must throwA[UnsupportedOperationException]

      }
    }

    "simple replication client" in {
      "get & set" in {
        // create my cluster client solely based on a zk client and a path
        val mycluster1 = CachePoolCluster.newZkCluster(firstPoolPath, zookeeperClient)
        Await.result(mycluster1.ready) // give it sometime for the cluster to get the initial set of memberships
        val mycluster2 = CachePoolCluster.newZkCluster(secondPoolPath, zookeeperClient)
        Await.result(mycluster2.ready) // give it sometime for the cluster to get the initial set of memberships

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
        Await.result(replicatedClient.get("foo")) mustEqual None
        Await.result(replicatedClient.set("foo", "bar")) mustNot throwA[Exception]
        Await.result(replicatedClient.get("foo")) mustEqual Some(stringToChannelBuffer("bar"))
        Await.result(client1.get("foo")) mustEqual Some(stringToChannelBuffer("bar"))
        Await.result(client2.get("foo")) mustEqual Some(stringToChannelBuffer("bar"))

        // inconsistent data
        Await.result(client2.set("client2-only", "test")) mustNot throwA[Exception]
        Await.result(client1.get("client2-only")) mustEqual None
        Await.result(client2.get("client2-only")) mustEqual Some(stringToChannelBuffer("test"))
        Await.result(replicatedClient.get("client2-only")) mustEqual Some(stringToChannelBuffer("test"))

        // set overwrites existing data
        Await.result(replicatedClient.set("client2-only", "test-again")) mustNot throwA[Exception]
        Await.result(replicatedClient.get("client2-only")) mustEqual Some(stringToChannelBuffer("test-again"))
        Await.result(client1.get("client2-only")) mustEqual Some(stringToChannelBuffer("test-again"))
        Await.result(client1.get("client2-only")) mustEqual Some(stringToChannelBuffer("test-again"))

        // inconsistent replica state
        ExternalMemcached.stop(firstPoolAddresses(0))
        ExternalMemcached.stop(firstPoolAddresses(1))
        Await.result(replicatedClient.set("foo", "baz")) must throwA[SimpleReplicationFailure]
        Await.result(replicatedClient.get("foo")) mustNot throwA[Exception]

        ExternalMemcached.stop(secondPoolAddresses(0))
        ExternalMemcached.stop(secondPoolAddresses(1))
        Await.result(replicatedClient.set("foo", "baz")) must throwA[SimpleReplicationFailure]
        Await.result(replicatedClient.get("foo")) must throwA[WriteException]
      }

      "gets & cas" in {
        // create my cluster client solely based on a zk client and a path
        val mycluster1 = CachePoolCluster.newZkCluster(firstPoolPath, zookeeperClient)
        Await.result(mycluster1.ready) // give it sometime for the cluster to get the initial set of memberships
        val mycluster2 = CachePoolCluster.newZkCluster(secondPoolPath, zookeeperClient)
        Await.result(mycluster2.ready) // give it sometime for the cluster to get the initial set of memberships

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
        Await.result(replicatedClient.gets("foo")) mustEqual None
        Await.result(replicatedClient.set("foo", "bar")) mustNot throwA[Exception]
        Await.result(replicatedClient.gets("foo")) mustEqual Some((stringToChannelBuffer("bar"), stringToChannelBuffer("1|1")))

        // inconsistent data
        Await.result(client1.set("inconsistent-key", "client1")) mustNot throwA[Exception]
        Await.result(client2.set("inconsistent-key", "client2")) mustNot throwA[Exception]
        Await.result(replicatedClient.gets("inconsistent-key")) mustEqual None

        // cas overwrites existing data
        Await.result(replicatedClient.cas("foo", "baz", stringToChannelBuffer("1|1"))) mustEqual true

        // inconsistent replica state
        ExternalMemcached.stop(firstPoolAddresses(0))
        ExternalMemcached.stop(firstPoolAddresses(1))
        Await.result(replicatedClient.cas("foo", "baz", stringToChannelBuffer("2|3"))) must throwA[SimpleReplicationFailure]
        Await.result(replicatedClient.gets("foo")) must throwA[SimpleReplicationFailure]
      }

      "delete" in {
        // create my cluster client solely based on a zk client and a path
        val mycluster1 = CachePoolCluster.newZkCluster(firstPoolPath, zookeeperClient)
        Await.result(mycluster1.ready) // give it sometime for the cluster to get the initial set of memberships
        val mycluster2 = CachePoolCluster.newZkCluster(secondPoolPath, zookeeperClient)
        Await.result(mycluster2.ready) // give it sometime for the cluster to get the initial set of memberships

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
        Await.result(replicatedClient.delete("empty-key")) mustEqual false

        Await.result(replicatedClient.set("foo", "bar")) mustNot throwA[Exception]
        Await.result(replicatedClient.get("foo")) mustEqual Some(stringToChannelBuffer("bar"))
        Await.result(client1.get("foo")) mustEqual Some(stringToChannelBuffer("bar"))
        Await.result(client1.get("foo")) mustEqual Some(stringToChannelBuffer("bar"))
        Await.result(replicatedClient.delete("foo")) mustEqual true
        Await.result(client1.get("foo")) mustEqual None
        Await.result(client2.get("foo")) mustEqual None

        // inconsistent data
        Await.result(client2.add("client2-only", "bar")) mustEqual true
        Await.result(client1.get("client2-only")) mustEqual None
        Await.result(client2.get("client2-only")) mustEqual Some(stringToChannelBuffer("bar"))
        Await.result(replicatedClient.delete("client2-only")) mustEqual false

        // inconsistent replica state
        Await.result(client2.set("client2-only", "bar")) mustEqual ()
        Await.result(client1.get("client2-only")) mustEqual None
        Await.result(client2.get("client2-only")) mustEqual Some(stringToChannelBuffer("bar"))
        ExternalMemcached.stop(firstPoolAddresses(0))
        ExternalMemcached.stop(firstPoolAddresses(1))
        Await.result(replicatedClient.delete("client2-only")) must throwA[SimpleReplicationFailure]
        ExternalMemcached.stop(secondPoolAddresses(0))
        ExternalMemcached.stop(secondPoolAddresses(1))
        Await.result(replicatedClient.delete("client2-only")) must throwA[SimpleReplicationFailure]
      }

      "add & replace" in {
        // create my cluster client solely based on a zk client and a path
        val mycluster1 = CachePoolCluster.newZkCluster(firstPoolPath, zookeeperClient)
        Await.result(mycluster1.ready) // give it sometime for the cluster to get the initial set of memberships
        val mycluster2 = CachePoolCluster.newZkCluster(secondPoolPath, zookeeperClient)
        Await.result(mycluster2.ready) // give it sometime for the cluster to get the initial set of memberships

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
        Await.result(replicatedClient.add("foo", "bar")) mustEqual true
        Await.result(replicatedClient.get("foo")) mustEqual Some(stringToChannelBuffer("bar"))
        Await.result(client1.get("foo")) mustEqual Some(stringToChannelBuffer("bar"))
        Await.result(client2.get("foo")) mustEqual Some(stringToChannelBuffer("bar"))

        Await.result(replicatedClient.replace("foo", "baz")) mustEqual true
        Await.result(client1.get("foo")) mustEqual Some(stringToChannelBuffer("baz"))
        Await.result(client2.get("foo")) mustEqual Some(stringToChannelBuffer("baz"))

        Await.result(replicatedClient.add("foo", "bar")) mustEqual false

        Await.result(replicatedClient.replace("no-such-key", "test")) mustEqual false

        // inconsistent data
        Await.result(client1.add("client1-only", "test")) mustEqual true
        Await.result(client2.add("client2-only", "test")) mustEqual true
        Await.result(replicatedClient.add("client2-only", "test")) mustEqual false
        Await.result(replicatedClient.replace("client1-only", "test")) mustEqual false

        // inconsistent replica state
        ExternalMemcached.stop(firstPoolAddresses(0))
        ExternalMemcached.stop(firstPoolAddresses(1))
        Await.result(replicatedClient.add("client2-only", "test")) must throwA[SimpleReplicationFailure]
        Await.result(replicatedClient.replace("client1-only", "test")) must throwA[SimpleReplicationFailure]
      }

      "indr & decr" in {
        // create my cluster client solely based on a zk client and a path
        val mycluster1 = CachePoolCluster.newZkCluster(firstPoolPath, zookeeperClient)
        Await.result(mycluster1.ready) // give it sometime for the cluster to get the initial set of memberships
        val mycluster2 = CachePoolCluster.newZkCluster(secondPoolPath, zookeeperClient)
        Await.result(mycluster2.ready) // give it sometime for the cluster to get the initial set of memberships

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
        Await.result(replicatedClient.incr("foo", 2L)) mustEqual None
        Await.result(replicatedClient.add("foo", "1")) mustEqual true
        Await.result(replicatedClient.get("foo")) mustEqual Some(stringToChannelBuffer("1"))
        Await.result(replicatedClient.incr("foo", 2L)) mustEqual Some(3L)
        Await.result(replicatedClient.decr("foo", 1L)) mustEqual Some(2L)

        // inconsistent data
        Await.result(client2.incr("foo", 1L)) mustEqual Some(3L)
        Await.result(replicatedClient.incr("foo", 2L)) mustEqual None
        Await.result(replicatedClient.decr("foo", 2L)) mustEqual None

        // inconsistent replica state
        ExternalMemcached.stop(firstPoolAddresses(0))
        ExternalMemcached.stop(firstPoolAddresses(1))
        Await.result(replicatedClient.incr("foo", 2L)) must throwA[SimpleReplicationFailure]
        Await.result(replicatedClient.decr("foo", 2L)) must throwA[SimpleReplicationFailure]
      }

      "many keys" in {
        // create my cluster client solely based on a zk client and a path
        val mycluster1 = CachePoolCluster.newZkCluster(firstPoolPath, zookeeperClient)
        Await.result(mycluster1.ready) // give it sometime for the cluster to get the initial set of memberships
        val mycluster2 = CachePoolCluster.newZkCluster(secondPoolPath, zookeeperClient)
        Await.result(mycluster2.ready) // give it sometime for the cluster to get the initial set of memberships

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
            Await.result(replicatedClient.set("foo"+n, "bar"+n))
          }
        }

        (0 until count).foreach {
          n => {
            Await.result(replicatedClient.get("foo"+n)) mustEqual Some(stringToChannelBuffer("bar"+n))
            Await.result(client1.get("foo"+n)) mustEqual Some(stringToChannelBuffer("bar"+n))
            Await.result(client2.get("foo"+n)) mustEqual Some(stringToChannelBuffer("bar"+n))
          }
        }

        // shutdown primary pool
        ExternalMemcached.stop(firstPoolAddresses(0))
        ExternalMemcached.stop(firstPoolAddresses(1))

        (0 until count).foreach {
          n => {
            Await.result(replicatedClient.get("foo"+n)) mustEqual Some(stringToChannelBuffer("bar"+n))
          }
        }
      }

      "replica down" in {
        // create my cluster client solely based on a zk client and a path
        val mycluster1 = CachePoolCluster.newZkCluster(firstPoolPath, zookeeperClient)
        Await.result(mycluster1.ready) // give it sometime for the cluster to get the initial set of memberships
        val mycluster2 = CachePoolCluster.newZkCluster(secondPoolPath, zookeeperClient)
        Await.result(mycluster2.ready) // give it sometime for the cluster to get the initial set of memberships

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

        Await.result(replicatedClient.set("foo", "bar"))
        Await.result(replicatedClient.get("foo")) mustEqual Some(stringToChannelBuffer("bar"))
        Await.result(client1.get("foo")) mustEqual Some(stringToChannelBuffer("bar"))
        Await.result(client2.get("foo")) mustEqual Some(stringToChannelBuffer("bar"))

        // primary pool down
        ExternalMemcached.stop(firstPoolAddresses(0))
        ExternalMemcached.stop(firstPoolAddresses(1))

        Await.result(replicatedClient.get("foo")) mustEqual Some(stringToChannelBuffer("bar"))
        Await.result(replicatedClient.set("foo", "baz")) must throwA[SimpleReplicationFailure] // set failed

        // bring back primary pool
        ExternalMemcached.start(firstPoolAddresses(0))
        ExternalMemcached.start(firstPoolAddresses(1))

        Await.result(replicatedClient.get("foo")) mustEqual Some(stringToChannelBuffer("baz"))
        Await.result(client1.get("foo")) mustEqual None
        Await.result(client2.get("foo")) mustEqual Some(stringToChannelBuffer("baz"))
        Await.result(replicatedClient.set("foo", "baz")) mustNot throwA[Exception]
      }

      "non supported operation" in {
        // create my cluster client solely based on a zk client and a path
        val mycluster1 = CachePoolCluster.newZkCluster(firstPoolPath, zookeeperClient)
        Await.result(mycluster1.ready) // give it sometime for the cluster to get the initial set of memberships
        val mycluster2 = CachePoolCluster.newZkCluster(secondPoolPath, zookeeperClient)
        Await.result(mycluster2.ready) // give it sometime for the cluster to get the initial set of memberships

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

        Await.result(replicatedClient.append("not-supported", "value")) must throwA[UnsupportedOperationException]
        Await.result(replicatedClient.prepend("not-supported", "value")) must throwA[UnsupportedOperationException]
      }
    }
  }

  "finagle 6 API" should {

    /**
     * Note: This integration test requires a real Memcached server to run.
     */
    var shutdownRegistry = new ShutdownRegistryImpl
    var memcachedAddress = List[Option[InetSocketAddress]]()

    var zkServerSetCluster: ZookeeperServerSetCluster = null
    var zookeeperClient: ZooKeeperClient = null
    val zkPath = "/cache/test/silly-cache"
    var zookeeperServer: ZooKeeperTestServer = null
    var zookeeperServerPort: Int = 0

    doBefore {
      // start zookeeper server and create zookeeper client
      zookeeperServer = new ZooKeeperTestServer(0, shutdownRegistry)
      zookeeperServer.startNetwork()
      zookeeperServerPort = zookeeperServer.getPort()

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
    }

    doAfter {
      zookeeperClient.close()

      // shutdown zookeeper server and client
      shutdownRegistry.execute()

      // shutdown memcached server
      ExternalMemcached.stop()
    }

    "with static servers list" in {
      val client = MemcachedClient.newKetamaClient(
        group = "twcache!localhost:%d,localhost:%d".format(memcachedAddress(0).get.getPort, memcachedAddress(1).get.getPort))

      Await.result(client.delete("foo"))
      Await.result(client.get("foo")) mustEqual None
      Await.result(client.set("foo", "bar"))
      Await.result(client.get("foo")).get.toString(CharsetUtil.UTF_8) mustEqual "bar"
    }

    "with managed cache pool" in {
      val client = MemcachedClient.newKetamaClient(
        group = "twcache!localhost:"+zookeeperServerPort+"!"+zkPath).asInstanceOf[PartitionedClient]

      client.delete("foo")()
      client.get("foo")() mustEqual None
      client.set("foo", "bar")()
      client.get("foo")().get.toString(CharsetUtil.UTF_8) mustEqual "bar"

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

    "with unmanaged regular zk serverset" in {
      val client = MemcachedClient.newKetamaClient(
        group = "zk!localhost:"+zookeeperServerPort+"!"+zkPath).asInstanceOf[PartitionedClient]

      // Wait for group to contain members
      Thread.sleep(5000)

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
  }
}
