package com.twitter.finagle.memcached.integration

import com.twitter.conversions.time._
import com.twitter.finagle.builder.{ClientBuilder, Server, ServerBuilder}
import com.twitter.finagle.memcached.Client
import com.twitter.finagle.memcached.protocol.text.Memcached
import com.twitter.finagle.memcached.protocol.{Command, Response}
import com.twitter.finagle.{Service, ServiceClosedException}
import com.twitter.io.Charsets
import com.twitter.util.{Await, RandomSocket}
import java.net.InetSocketAddress
import org.jboss.netty.buffer.ChannelBuffers
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}

@RunWith(classOf[JUnitRunner])
class ProxyTest extends FunSuite with BeforeAndAfter {

  type MemcacheService = Service[Command, Response]
  /**
    * Note: This integration test requires a real Memcached server to run.
    */
  var externalClient: Client = null
  var server: Server = null
  var serverPort: InetSocketAddress = null
  var proxyService: MemcacheService = null
  var proxyClient: MemcacheService = null
  var testServer: Option[TestMemcachedServer] = None

  before {
    testServer = TestMemcachedServer.start()
    if (testServer.isDefined) {
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
  }

  after {
    // externalClient.close() needs to be called explicitly by each test. Otherwise
    // 'quit' test would call it twice.
    if (testServer.isDefined) {
      server.close(0.seconds)
      proxyService.close()
      proxyClient.close()
      testServer map { _.stop() }
    }
  }

  override def withFixture(test: NoArgTest) = {
    if (testServer == None) info("Cannot start memcached. skipping test...")
    else test()
  }

  test("Proxied Memcached Servers should handle a basic get/set operation") {
    Await.result(externalClient.delete("foo"))
    assert(Await.result(externalClient.get("foo")) === None)
    Await.result(externalClient.set("foo", ChannelBuffers.wrappedBuffer("bar".getBytes(Charsets.Utf8))))
    val foo = Await.result(externalClient.get("foo"))
    assert(foo.isDefined)
    assert(foo.get.toString(Charsets.Utf8) === "bar")
    externalClient.release()
  }

  if (Option(System.getProperty("USE_EXTERNAL_MEMCACHED")).isDefined) {
    test("stats is supported") {
      Await.result(externalClient.delete("foo"))
      assert(Await.result(externalClient.get("foo")) === None)
      Await.result(externalClient.set("foo", ChannelBuffers.wrappedBuffer("bar".getBytes(Charsets.Utf8))))
      Seq(None, Some("slabs")).foreach { arg =>
        val stats = Await.result(externalClient.stats(arg))
        assert(stats != null)
        assert(!stats.isEmpty)
        stats.foreach { line =>
          assert(line.startsWith("STAT"))
        }
      }
      externalClient.release()
    }
  }

  if (Option(System.getProperty("USE_EXTERNAL_MEMCACHED")).isDefined) {
    test("stats (cachedump) is supported") {
      Await.result(externalClient.delete("foo"))
      assert(Await.result(externalClient.get("foo")) === None)
      Await.result(externalClient.set("foo", ChannelBuffers.wrappedBuffer("bar".getBytes(Charsets.Utf8))))
      val slabs = Await.result(externalClient.stats(Some("slabs")))
      assert(slabs != null)
      assert(!slabs.isEmpty)
      val n = slabs.head.split(" ")(1).split(":")(0).toInt
      val stats = Await.result(externalClient.stats(Some("cachedump " + n + " 100")))
      assert(stats != null)
      assert(!stats.isEmpty)
      stats.foreach { stat =>
        assert(stat.startsWith("ITEM"))
      }
      assert(stats.find { stat => stat.contains("foo") } isDefined)
      externalClient.release()
    }
  }

  test("quit is supported") {
    Await.result(externalClient.get("foo")) // do nothing
    Await.result(externalClient.quit())
    intercept[ServiceClosedException] {
      Await.result(externalClient.get("foo"))
    }
  }

}
