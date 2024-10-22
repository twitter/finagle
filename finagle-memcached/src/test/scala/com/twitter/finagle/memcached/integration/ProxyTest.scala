package com.twitter.finagle.memcached.integration

import com.twitter.conversions.DurationOps._
import com.twitter.finagle._
import com.twitter.finagle.memcached.Client
import com.twitter.finagle.memcached.integration.external.TestMemcachedServer
import com.twitter.finagle.memcached.protocol.Command
import com.twitter.finagle.memcached.protocol.Response
import com.twitter.io.Buf
import com.twitter.util.Await
import com.twitter.util.Awaitable
import java.net.InetAddress
import java.net.InetSocketAddress
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.funsuite.AnyFunSuite

class ProxyTest extends AnyFunSuite with BeforeAndAfter {

  val TimeOut = 15.seconds

  private def awaitResult[T](awaitable: Awaitable[T]): T = Await.result(awaitable, TimeOut)

  type MemcacheService = Service[Command, Response]

  /**
   * Note: This integration test requires a real Memcached server to run.
   */
  var externalClient: Client = null
  var server: ListeningServer = null
  var serverAddress: InetSocketAddress = null
  var proxyService: MemcacheService = null
  var proxyClient: MemcacheService = null
  var testServer: Option[TestMemcachedServer] = None

  before {
    testServer = TestMemcachedServer.start()
    if (testServer.isDefined) {
      Thread.sleep(150) // On my box the 100ms sleep wasn't long enough
      proxyClient = Memcached.client
        .connectionsPerEndpoint(1)
        .newService(
          Name.bound(Address(testServer.get.address.asInstanceOf[InetSocketAddress])),
          "memcached"
        )

      proxyService = new MemcacheService {
        def apply(request: Command) = proxyClient(request)
      }

      server = Memcached.server
        .withLabel("memcached")
        .serve(new InetSocketAddress(InetAddress.getLoopbackAddress, 0), proxyService)

      serverAddress = server.boundAddress.asInstanceOf[InetSocketAddress]
      externalClient = Client(
        Memcached.client
          .newService("%s:%d".format(serverAddress.getHostName, serverAddress.getPort))
      )
    }
  }

  after {
    // externalClient.close() needs to be called explicitly by each test. Otherwise
    // 'quit' test would call it twice.
    if (testServer.isDefined) {
      server.close(0.seconds)
      proxyService.close()
      proxyClient.close()
      testServer.map(_.stop())
    }
  }

  override def withFixture(test: NoArgTest) = {
    if (testServer == None) {
      info("Cannot start memcached. skipping test...")
      cancel()
    } else test()
  }

  test("Proxied Memcached Servers should handle a basic get/set operation") {
    awaitResult(externalClient.delete("foo"))
    assert(awaitResult(externalClient.get("foo")) == None)
    awaitResult(externalClient.set("foo", Buf.Utf8("bar")))
    val foo = awaitResult(externalClient.get("foo"))
    assert(foo.isDefined)
    val Buf.Utf8(res) = foo.get
    assert(res == "bar")
    awaitResult(externalClient.close())
  }

  if (Option(System.getProperty("EXTERNAL_MEMCACHED_PATH")).isDefined) {
    test("stats is supported") {
      awaitResult(externalClient.delete("foo"))
      assert(awaitResult(externalClient.get("foo")) == None)
      awaitResult(externalClient.set("foo", Buf.Utf8("bar")))
      Seq(None, Some("slabs")).foreach { arg =>
        val stats = awaitResult(externalClient.stats(arg))
        assert(stats != null)
        assert(!stats.isEmpty)
        stats.foreach { line => assert(line.startsWith("STAT")) }
      }
      awaitResult(externalClient.close())
    }
  }

  if (Option(System.getProperty("EXTERNAL_MEMCACHED_PATH")).isDefined) {
    test("stats (cachedump) is supported") {
      awaitResult(externalClient.delete("foo"))
      assert(awaitResult(externalClient.get("foo")) == None)
      awaitResult(externalClient.set("foo", Buf.Utf8("bar")))
      val slabs = awaitResult(externalClient.stats(Some("slabs")))
      assert(slabs != null)
      assert(!slabs.isEmpty)
      val n = slabs.head.split(" ")(1).split(":")(0).toInt

      eventually {
        val stats = awaitResult(externalClient.stats(Some("cachedump " + n + " 100")))
        assert(stats != null)
        assert(!stats.isEmpty)
        stats.foreach { stat => assert(stat.startsWith("ITEM")) }
        assert(stats.find { stat => stat.contains("foo") }.isDefined)
      }

      awaitResult(externalClient.close())
    }
  }

  test("quit is supported") {
    awaitResult(externalClient.get("foo")) // do nothing
    awaitResult(externalClient.quit())
    intercept[ServiceClosedException] {
      awaitResult(externalClient.get("foo"))
    }
  }

}
