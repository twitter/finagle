package com.twitter.finagle.memcached.integration

import com.twitter.finagle._
import com.twitter.conversions.DurationOps._
import com.twitter.finagle.memcached.compressing.scheme.Lz4
import com.twitter.finagle.memcached.compressing.scheme.Uncompressed
import com.twitter.finagle.memcached.integration.external.InProcessMemcached
import com.twitter.finagle.memcached.protocol.Gets
import com.twitter.finagle.memcached.protocol.Response
import com.twitter.finagle.memcached.protocol.Values
import com.twitter.finagle.memcached.protocol.ValuesAndErrors
import com.twitter.finagle.partitioning.param
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.hashing.KeyHasher
import com.twitter.io.Buf
import com.twitter.util.Await
import com.twitter.util.Awaitable
import com.twitter.util.Duration
import java.net.InetAddress
import java.net.InetSocketAddress
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class MemcachedCompressingClientTest extends AnyFunSuite with BeforeAndAfter {

  val clientName = "test_client"
  val Timeout: Duration = 15.seconds
  val stats = new InMemoryStatsReceiver

  def awaitResult[T](awaitable: Awaitable[T]): T = Await.result(awaitable, Timeout)

  private[this] val alwaysCompressedData =
    Buf.ByteArray.Owned(Array.fill[Byte](1024)(0))

  private[this] val neverCompressedData = Buf.Utf8("boing")

  private def getResponseBuf(response: Response): Seq[Buf] = {
    response match {
      case Values(values) => values.map(_.value)
      case ValuesAndErrors(values, _) => values.map(_.value)
      case _ => Seq.empty
    }
  }

  after {
    stats.clear()
  }

  test("withCompressionScheme Lz4 and toggled on") {

    val server = new InProcessMemcached(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))
    val address = Address(server.start().boundAddress.asInstanceOf[InetSocketAddress])

    val client = Memcached.client
      .configured(param.KeyHasher(KeyHasher.FNV1_32))
      .connectionsPerEndpoint(1)
      .withStatsReceiver(stats)
      .withCompressionScheme(Lz4)
      .newRichClient(Name.bound(address), clientName)

    awaitResult(client.set("foobar", alwaysCompressedData)) // will be compressed
    awaitResult(client.set("baz", neverCompressedData)) // won't be compressed

    val alwaysCompressedServiceResponse: Response =
      awaitResult[Response](server.service(Gets(Seq(Buf.Utf8("foobar")))))
    val neverCompressedServiceResponse = awaitResult(server.service(Gets(Seq(Buf.Utf8("baz")))))

    val alwaysCompressedServiceData = getResponseBuf(alwaysCompressedServiceResponse).head
    val neverCompressedServiceData = getResponseBuf(neverCompressedServiceResponse).head

    val results = awaitResult(
      client.gets(Seq("foobar", "baz"))
    ).flatMap {
      case (key, (value1, _)) =>
        Map((key, value1))
    }

    val deletedResult = awaitResult {
      for {
        _ <- client.delete("foobar")
        _ <- client.delete("baz")
        r <- client.gets(Seq("foobar", "baz"))
      } yield r
    }

    assert(results("foobar") === alwaysCompressedData)
    assert(results("baz") === neverCompressedData)
    assert(deletedResult.isEmpty)
    assert(alwaysCompressedData.length > alwaysCompressedServiceData.length)
    assert(neverCompressedData.length == neverCompressedServiceData.length)

    assert(stats.counters(Seq(clientName, "lz4", "decompression", "attempted")) == 1)
    assert(stats.counters(Seq(clientName, "lz4", "compression", "attempted")) == 1)
    assert(stats.counters(Seq(clientName, "lz4", "compression", "skipped")) == 1)

    client.close()
    server.stop()
  }

  test("withCompressionScheme Uncompressed") {

    val server = new InProcessMemcached(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))
    val address = Address(server.start().boundAddress.asInstanceOf[InetSocketAddress])

    val client = Memcached.client
      .configured(param.KeyHasher(KeyHasher.FNV1_32))
      .connectionsPerEndpoint(1)
      .withStatsReceiver(stats)
      .withCompressionScheme(Uncompressed)
      .newRichClient(Name.bound(address), clientName)

    awaitResult(client.set("foobar", alwaysCompressedData)) // will be compressed
    awaitResult(client.set("baz", neverCompressedData)) // won't be compressed

    val alwaysCompressedServiceResponse: Response =
      awaitResult[Response](server.service(Gets(Seq(Buf.Utf8("foobar")))))
    val neverCompressedServiceResponse = awaitResult(server.service(Gets(Seq(Buf.Utf8("baz")))))

    val alwaysCompressedServiceData = getResponseBuf(alwaysCompressedServiceResponse).head
    val neverCompressedServiceData = getResponseBuf(neverCompressedServiceResponse).head

    val results = awaitResult(
      client.gets(Seq("foobar", "baz"))
    ).flatMap {
      case (key, (value1, _)) =>
        Map((key, value1))
    }

    val deletedResult = awaitResult {
      for {
        _ <- client.delete("foobar")
        _ <- client.delete("baz")
        r <- client.gets(Seq("foobar", "baz"))
      } yield r
    }

    assert(results("foobar") === alwaysCompressedData)
    assert(results("baz") === neverCompressedData)
    assert(deletedResult.isEmpty)
    assert(alwaysCompressedData.length == alwaysCompressedServiceData.length)
    assert(neverCompressedData.length == neverCompressedServiceData.length)

    assert(stats.counters(Seq(clientName, "lz4", "decompression", "attempted")) == 0)
    assert(stats.counters(Seq(clientName, "lz4", "compression", "attempted")) == 0)
    assert(!stats.counters.contains(Seq(clientName, "uncompressed", "decompression", "attempted")))
    assert(!stats.counters.contains(Seq(clientName, "uncompressed", "compression", "attempted")))

    client.close()
    server.stop()
  }

  test("Clients with different compression schemes can decompress each other's values") {

    val server = new InProcessMemcached(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))
    val address = Address(server.start().boundAddress.asInstanceOf[InetSocketAddress])

    val compressionClient = Memcached.client
      .configured(param.KeyHasher(KeyHasher.FNV1_32))
      .connectionsPerEndpoint(1)
      .withStatsReceiver(stats)
      .withCompressionScheme(Lz4)
      .newRichClient(Name.bound(address), clientName)

    val uncompressedClient = Memcached.client
      .configured(param.KeyHasher(KeyHasher.FNV1_32))
      .connectionsPerEndpoint(1)
      .withStatsReceiver(stats)
      .newRichClient(Name.bound(address), clientName)

    awaitResult(uncompressedClient.set("foobar", alwaysCompressedData))
    awaitResult(uncompressedClient.set("baz", neverCompressedData))

    awaitResult(compressionClient.set("foo", alwaysCompressedData))
    awaitResult(compressionClient.set("bazbar", neverCompressedData))

    val compressionClientResults = awaitResult(
      compressionClient.gets(Seq("foobar", "baz"))
    ).flatMap {
      case (key, (value1, _)) =>
        Map((key, value1))
    }

    val clientResults = awaitResult(
      uncompressedClient.gets(Seq("foo", "bazbar"))
    ).flatMap {
      case (key, (value1, _)) =>
        Map((key, value1))
    }

    assert(compressionClientResults("foobar") === alwaysCompressedData)
    assert(compressionClientResults("baz") === neverCompressedData)
    assert(clientResults("foo") === alwaysCompressedData)
    assert(clientResults("bazbar") === neverCompressedData)

    uncompressedClient.close()
    compressionClient.close()
    server.stop()
  }
}
