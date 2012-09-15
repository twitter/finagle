package com.twitter.finagle.memcached.integration

/* temporarily disabled due to internal dependency

import com.twitter.common.application.ShutdownRegistry.ShutdownRegistryImpl
import com.twitter.common.zookeeper.ZooKeeperClient
import com.twitter.common.zookeeper.testing.ZooKeeperTestServer
import com.twitter.common_internal.zookeeper.TwitterServerSet
import com.twitter.common_internal.zookeeper.TwitterServerSet.Service
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.memcached.{PartitionedClient, Server, Client, KetamaClientBuilder}
import com.twitter.finagle.memcached.protocol._
import com.twitter.finagle.memcached.protocol.text.Memcached
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import com.twitter.finagle.memcached.{Server, Client, KetamaClientBuilder}
import com.twitter.finagle.stats.SummarizingStatsReceiver
import com.twitter.finagle.tracing.ConsoleTracer
import com.twitter.finagle.zookeeper.ZookeeperServerSetCluster
import java.net.InetSocketAddress
import org.jboss.netty.util.CharsetUtil
import org.specs.SpecificationWithJUnit

class ClientSpec extends SpecificationWithJUnit {
  "ConnectedClient" should {
    /**
     * Note: This integration test requires a real Memcached server to run.
     */
    var client: Client = null

    val stats = new SummarizingStatsReceiver

    doBefore {
      var address: Option[InetSocketAddress] = ExternalMemcached.start()
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
    }
  }

  "Cluster client" should {

    /**
     * Note: This integration test requires a real Memcached server to run.
     */
    var shutdownRegistry = new ShutdownRegistryImpl
    var memcachedAddress = List[Option[InetSocketAddress]]()

    var zookeeperClient: ZooKeeperClient = null
    val sdService: Service = new Service("cache","test","silly-cache")

    doBefore {
      // start zookeeper server and create zookeeper client
      var zookeeperServer = new ZooKeeperTestServer(0, shutdownRegistry)
      zookeeperServer.startNetwork()

      // connect to zookeeper server
      zookeeperClient = zookeeperServer.createClient(
        ZooKeeperClient.digestCredentials(sdService.getRole(), sdService.getRole()))

      // create serverset
      val serverSet = TwitterServerSet.create(zookeeperClient, sdService)
      val zkServerSetCluster = new ZookeeperServerSetCluster(serverSet)

      // start five memcached server and join the cluster
      (0 to 4) foreach { _ =>
        val address: Option[InetSocketAddress] = ExternalMemcached.start()
        memcachedAddress ::= address
        zkServerSetCluster.join(address.get)
      }

      Thread.sleep(1000) // give it some time for the zookeeper change to be reflected in the cluster

    }

    doAfter {
      // shutdown zookeeper server and client
      shutdownRegistry.execute()

      // shutdown memcached server
      ExternalMemcached.stop()
    }

    "Simple ClusterClient using finagle load balancing" in {
      "many keys" in {
        // create simple cluster client
        val mycluster =
          new ZookeeperServerSetCluster(TwitterServerSet.create(zookeeperClient, sdService))
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
                .tracerFactory(ConsoleTracer.factory)
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

    "Ketama ClusterClient using a distributor" in {
      "set & get" in {
        // create my cluster client solely based on a zk client and a path
        val mycluster =
          new ZookeeperServerSetCluster(TwitterServerSet.create(zookeeperClient, sdService)) map {
            socketAddr =>
              socketAddr.asInstanceOf[InetSocketAddress]
          }
        mycluster.ready() // give it sometime for the cluster to get the initial set of memberships

        val client = KetamaClientBuilder()
                .cluster(mycluster)
                .build()

        client.delete("foo")()
        client.get("foo")() mustEqual None
        client.set("foo", "bar")()
        client.get("foo")().get.toString(CharsetUtil.UTF_8) mustEqual "bar"

      }

      "many keys" in {
        // create my cluster client solely based on a zk client and a path
        val mycluster =
          new ZookeeperServerSetCluster(TwitterServerSet.create(zookeeperClient, sdService)) map {
            socketAddr =>
              socketAddr.asInstanceOf[InetSocketAddress]
          }
        mycluster.ready() // give it sometime for the cluster to get the initial set of memberships

        val client = KetamaClientBuilder()
                .cluster(mycluster)
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
    }
  }
}

*/
