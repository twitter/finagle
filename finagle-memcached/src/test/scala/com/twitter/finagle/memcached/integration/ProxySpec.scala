package com.twitter.finagle.memcached.integration

import org.specs.Specification

import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.util.CharsetUtil

import com.twitter.conversions.time._
import com.twitter.finagle.memcached.Client
import com.twitter.finagle.memcached.protocol.text.Memcached
import com.twitter.finagle.memcached.protocol.{Command, Response}
import com.twitter.finagle.Service
import com.twitter.finagle.ServiceClosedException
import com.twitter.finagle.builder.{ClientBuilder, Server, ServerBuilder}
import com.twitter.util.RandomSocket

import java.net.{InetSocketAddress, Socket}

object ProxySpec extends Specification {

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

    doBefore {
      ExternalMemcached.start();
      Thread.sleep(150) // On my box the 100ms sleep wasn't long enough
      proxyClient = ClientBuilder()
        .hosts(Seq(ExternalMemcached.address.get))
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
      // externalClient.release() needs to be called explicitly by each test. Otherwise
      // 'quit' test would call it twice.
      server.close(0.seconds)
      proxyService.release()
      proxyClient.release()
      ExternalMemcached.stop()
    }

    "handle a basic get/set operation" in {
      externalClient.delete("foo")()
      externalClient.get("foo")() must beNone
      externalClient.set("foo", ChannelBuffers.wrappedBuffer("bar".getBytes(CharsetUtil.UTF_8)))()
      val foo = externalClient.get("foo")()
      foo must beSome
      foo.get.toString(CharsetUtil.UTF_8) mustEqual "bar"
      externalClient.release()
    }

    "stats is supported" in {
      externalClient.delete("foo")()
      externalClient.get("foo")() must beNone
      externalClient.set("foo", ChannelBuffers.wrappedBuffer("bar".getBytes(CharsetUtil.UTF_8)))()
      Seq(None, Some("items"), Some("slabs")).foreach { arg =>
        val stats = externalClient.stats(arg)()
        stats must notBeEmpty
        stats.foreach { line =>
          line must startWith("STAT")
        }
      }
      externalClient.release()
    }

    "stats is supported (no value)" in {
      val stats = externalClient.stats("items")()
      stats must beEmpty
      externalClient.release()
    }

    "stats (cachedump) is supported" in {
      externalClient.delete("foo")()
      externalClient.get("foo")() must beNone
      externalClient.set("foo", ChannelBuffers.wrappedBuffer("bar".getBytes(CharsetUtil.UTF_8)))()
      val slabs = externalClient.stats(Some("slabs"))()
      slabs must notBeEmpty
      val n = slabs.head.split(" ")(1).split(":")(0).toInt
      val stats = externalClient.stats(Some("cachedump " + n + " 100"))()
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
      externalClient.get("foo")() // do nothing
      externalClient.quit()()
      externalClient.get("foo")() must throwA[ServiceClosedException]
    }

  }

}
