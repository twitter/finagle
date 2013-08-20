package com.twitter.finagle.memcached.integration

import com.twitter.conversions.time._
import com.twitter.finagle.builder.{ClientBuilder, Server, ServerBuilder}
import com.twitter.finagle.memcached.Client
import com.twitter.finagle.memcached.protocol.text.Memcached
import com.twitter.finagle.memcached.protocol.{Command, Response}
import com.twitter.finagle.{Service, ServiceClosedException}
import com.twitter.util.{Await, RandomSocket}
import java.net.InetSocketAddress
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.util.CharsetUtil
import org.specs.SpecificationWithJUnit

class ProxySpec extends SpecificationWithJUnit {

  type MemcacheService = Service[Command, Response]

  "Proxied Memcached Servers" should {
    /**
     * Note: This integration test requires a real Memcached server to run.
     */
    var externalClient: Client = null
    var server: Server = null
    var serverPort: InetSocketAddress = null
    var proxyService: MemcacheService = null
    var proxyClient: MemcacheService = null
    var testServer: Option[TestMemcachedServer] = None

    doBefore {
      testServer = TestMemcachedServer.start()
      if (testServer == None) skip("Cannot start memcached. skipping...")
      Thread.sleep(150) // On my box the 100ms sleep wasn't long enough
      proxyClient = ClientBuilder()
        .hosts(Seq(testServer.get.address))
        .codec(Memcached())
        .hostConnectionLimit(1)
        .build()
      proxyService = new MemcacheService {
        def apply(request: Command) = proxyClient(request)
      }
      serverPort = RandomSocket()
      server = ServerBuilder()
        .codec(Memcached())
        .bindTo(serverPort)
        .name("memcached")
        .build(proxyService)
      externalClient = Client("%s:%d".format(serverPort.getHostName, serverPort.getPort))
    }

    doAfter {
      // externalClient.close() needs to be called explicitly by each test. Otherwise
      // 'quit' test would call it twice.
      server.close(0.seconds)
      proxyService.close()
      proxyClient.close()
      testServer map { _.stop() }
    }

    "handle a basic get/set operation" in {
      Await.result(externalClient.delete("foo"))
      Await.result(externalClient.get("foo")) must beNone
      Await.result(externalClient.set("foo", ChannelBuffers.wrappedBuffer("bar".getBytes(CharsetUtil.UTF_8))))
      val foo = Await.result(externalClient.get("foo"))
      foo must beSome
      foo.get.toString(CharsetUtil.UTF_8) mustEqual "bar"
      externalClient.release()
    }

    if (Option(System.getProperty("USE_EXTERNAL_MEMCACHED")).isDefined) "stats is supported" in {
      Await.result(externalClient.delete("foo"))
      Await.result(externalClient.get("foo")) must beNone
      Await.result(externalClient.set("foo", ChannelBuffers.wrappedBuffer("bar".getBytes(CharsetUtil.UTF_8))))
      Seq(None, Some("slabs")).foreach { arg =>
        val stats = Await.result(externalClient.stats(arg))
        stats must notBeEmpty
        stats.foreach { line =>
          line must startWith("STAT")
        }
      }
      externalClient.release()
    }

    if (Option(System.getProperty("USE_EXTERNAL_MEMCACHED")).isDefined) "stats (cachedump) is supported" in {
      Await.result(externalClient.delete("foo"))
      Await.result(externalClient.get("foo")) must beNone
      Await.result(externalClient.set("foo", ChannelBuffers.wrappedBuffer("bar".getBytes(CharsetUtil.UTF_8))))
      val slabs = Await.result(externalClient.stats(Some("slabs")))
      slabs must notBeEmpty
      val n = slabs.head.split(" ")(1).split(":")(0).toInt
      val stats = Await.result(externalClient.stats(Some("cachedump " + n + " 100")))
      stats must notBeEmpty
      stats.foreach { stat =>
        stat must startWith("ITEM")
      }
      stats.find { stat =>
        stat.contains("foo")
      } must beSome
      externalClient.release()
    }

    "quit is supported" in {
      Await.result(externalClient.get("foo")) // do nothing
      Await.result(externalClient.quit())
      Await.result(externalClient.get("foo")) must throwA[ServiceClosedException]
    }

  }
}
