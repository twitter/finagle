package com.twitter.finagle.memcached.integration

import com.twitter.common.io.FileUtils._
import com.twitter.common.quantity.{Time, Amount}
import com.twitter.common.zookeeper.{ServerSetImpl, ZooKeeperClient}
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.memcached.{PartitionedClient, Server, Client, KetamaClientBuilder}
import com.twitter.finagle.memcached.protocol._
import com.twitter.finagle.memcached.protocol.text.Memcached
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import com.twitter.finagle.memcached.{Server, Client, KetamaClientBuilder}
import com.twitter.finagle.stats.SummarizingStatsReceiver
import com.twitter.finagle.tracing.ConsoleTracer
import com.twitter.finagle.zookeeper.ZookeeperServerSetCluster
import java.net.{InetSocketAddress, SocketAddress}
import org.jboss.netty.util.CharsetUtil
import org.specs.SpecificationWithJUnit
import org.apache.zookeeper.server.{ZooKeeperServer, NIOServerCnxn}
import org.apache.zookeeper.server.persistence.FileTxnSnapLog

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
        client.get("fo o")() must throwA[ClientError]
        client.set("", "bar")() must throwA[ClientError]
        client.get("    foo")() must throwA[ClientError]
        client.get("foo   ")() must throwA[ClientError]
        client.get("    foo")() must throwA[ClientError]
        val nullString: String = null
        client.get(nullString)() must throwA[ClientError]
        client.set(nullString, "bar")() must throwA[ClientError]
        client.set("    ", "bar")() must throwA[ClientError]
        client.set("\t", "bar")() must throwA[ClientError]
        client.set("\r", "bar")() must throwA[ClientError]
        client.set("\n", "bar")() must throwA[ClientError]
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
    val zookeeperAddress = new InetSocketAddress(2181)
    var connectionFactory: NIOServerCnxn.Factory = null
    var memcachedAddress = List[Option[InetSocketAddress]]()

    var zookeeperClient: ZooKeeperClient = null
    val zkPath = "/twitter/services/cache/silly-cache"

    doBefore {
      // start zookeeper server and create zookeeper client
      val zookeeperServer = new ZooKeeperServer(
        new FileTxnSnapLog(createTempDir(), createTempDir()),
        new ZooKeeperServer.BasicDataTreeBuilder)
      connectionFactory = new NIOServerCnxn.Factory(zookeeperAddress)
      connectionFactory.startup(zookeeperServer)

      // connect to zookeeper server
      zookeeperClient = new ZooKeeperClient(
        Amount.of(100, Time.MILLISECONDS),
        zookeeperAddress)

      // create serverset
      val serverSet = new ServerSetImpl(zookeeperClient, zkPath)
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
      // shutdown zookeeper server
      connectionFactory.shutdown()
      zookeeperClient.close()

      // shutdown memcached server
      ExternalMemcached.stop()
    }

    "Simple ClusterClient using finagle load balancing" in {
      "many keys" in {
        // create simple cluster client
        val mycluster = new ZookeeperServerSetCluster(new ServerSetImpl(zookeeperClient, zkPath))
        Thread.sleep(1000) // give it sometime for the cluster to get the initial set of memberships
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
          new ZookeeperServerSetCluster(new ServerSetImpl(zookeeperClient, zkPath)) map {
            socketAddr =>
              socketAddr.asInstanceOf[InetSocketAddress]
          }
        Thread.sleep(1000) // give it sometime for the cluster to get the initial set of memberships

        val client = KetamaClientBuilder()
                .cluster(mycluster)
                .build()
                .asInstanceOf[PartitionedClient]

        client.delete("foo")()
        client.get("foo")() mustEqual None
        client.set("foo", "bar")()
        client.get("foo")().get.toString(CharsetUtil.UTF_8) mustEqual "bar"

      }

      "many keys" in {
        // create my cluster client solely based on a zk client and a path
        val mycluster =
          new ZookeeperServerSetCluster(new ServerSetImpl(zookeeperClient, zkPath)) map {
            socketAddr =>
              socketAddr.asInstanceOf[InetSocketAddress]
          }
        Thread.sleep(1000) // give it sometime for the cluster to get the initial set of memberships

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
