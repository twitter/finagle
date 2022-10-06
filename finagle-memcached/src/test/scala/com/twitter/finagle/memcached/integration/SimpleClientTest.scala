package com.twitter.finagle.memcached.integration

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Address
import com.twitter.finagle.Memcached
import com.twitter.finagle.Name
import com.twitter.finagle.param
import com.twitter.finagle.memcached.Client
import com.twitter.finagle.memcached.integration.external.TestMemcachedServer
import com.twitter.finagle.memcached.protocol._
import com.twitter.finagle.partitioning.zk.ZkMetadata
import com.twitter.finagle.stats.SummarizingStatsReceiver
import com.twitter.finagle.tracing._
import com.twitter.io.Buf
import com.twitter.util.registry.Entry
import com.twitter.util.registry.GlobalRegistry
import com.twitter.util.registry.SimpleRegistry
import com.twitter.util.Await
import com.twitter.util.Awaitable
import java.net.InetSocketAddress
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.atLeastOnce
import org.mockito.Mockito.spy
import org.mockito.Mockito.verify
import org.mockito.Mockito.when
import org.scalatest.BeforeAndAfter
import org.scalatest.Outcome
import scala.collection.JavaConverters._
import org.scalatest.funsuite.AnyFunSuite

class SimpleClientTest extends AnyFunSuite with BeforeAndAfter {

  /**
   * Note: This integration test requires a real Memcached server to run.
   */
  private var client: Client = null
  private var testServer: Option[TestMemcachedServer] = None

  private val stats = new SummarizingStatsReceiver

  private def awaitResult[T](awaitable: Awaitable[T]): T = Await.result(awaitable, 5.seconds)

  private val baseClient: Memcached.Client = Memcached.client

  before {
    testServer = TestMemcachedServer.start()
    if (testServer.isDefined)
      client = newClient(baseClient)
  }

  after {
    if (testServer.isDefined)
      testServer map { _.stop() }
  }

  def newClient(baseClient: Memcached.Client): Client = {
    // we add zk metadata to with a shard_id 0
    val address = Address.Inet(
      testServer.get.address.asInstanceOf[InetSocketAddress],
      ZkMetadata.toAddrMetadata(ZkMetadata(Some(0)))
    )

    val service = baseClient
      .withStatsReceiver(stats)
      .connectionsPerEndpoint(1)
      .newService(Name.bound(address), "memcache")
    Client(service)
  }

  override def withFixture(test: NoArgTest): Outcome = {
    if (testServer.isDefined) test()
    else {
      info("Cannot start memcached. Skipping test...")
      cancel()
    }
  }

  def withExpectedTraces(f: Client => Unit, expected: Seq[Annotation]): Unit = {
    val tracer = spy(new NullTracer)
    when(tracer.isActivelyTracing(any[TraceId])).thenReturn(true)
    when(tracer.isNull).thenReturn(false)
    val captor: ArgumentCaptor[Record] = ArgumentCaptor.forClass(classOf[Record])

    val client = newClient(baseClient.configured(param.Tracer(tracer)))
    f(client)
    verify(tracer, atLeastOnce()).record(captor.capture())
    val annotations = captor.getAllValues.asScala collect { case Record(_, _, a, _) => a }
    assert(expected.filterNot(annotations.contains(_)).isEmpty)
  }

  test("set & get") {
    awaitResult(client.delete("foo"))
    assert(awaitResult(client.get("foo")) == None)
    awaitResult(client.set("foo", Buf.Utf8("bar")))
    assert(awaitResult(client.get("foo")).get == Buf.Utf8("bar"))
  }

  test("get") {
    awaitResult(client.set("foo", Buf.Utf8("bar")))
    awaitResult(client.set("baz", Buf.Utf8("boing")))
    val result = awaitResult(client.get(Seq("foo", "baz", "notthere")))
      .map { case (key, Buf.Utf8(value)) => (key, value) }
    assert(
      result == Map(
        "foo" -> "bar",
        "baz" -> "boing"
      )
    )
  }

  if (Option(System.getProperty("USE_EXTERNAL_MEMCACHED")).isDefined) {
    test("gets") {
      awaitResult(client.set("foos", Buf.Utf8("xyz")))
      awaitResult(client.set("bazs", Buf.Utf8("xyz")))
      awaitResult(client.set("bazs", Buf.Utf8("zyx")))
      val result = awaitResult(client.gets(Seq("foos", "bazs", "somethingelse")))
        .map {
          case (key, (Buf.Utf8(value), Buf.Utf8(casUnique))) =>
            (key, (value, casUnique))
        }

      assert(
        result == Map(
          "foos" -> (
            (
              "xyz",
              "1"
            )
          ), // the "cas unique" values are predictable from a fresh memcached
          "bazs" -> (("zyx", "3"))
        )
      )
    }
  }

  if (Option(System.getProperty("USE_EXTERNAL_MEMCACHED")).isDefined) {
    test("cas") {
      awaitResult(client.set("x", Buf.Utf8("y")))
      val Some((value, casUnique)) = awaitResult(client.gets("x"))
      assert(value == Buf.Utf8("y"))
      assert(casUnique == Buf.Utf8("1"))

      assert(!awaitResult(client.checkAndSet("x", Buf.Utf8("z"), Buf.Utf8("2")).map(_.replaced)))
      assert(
        awaitResult(client.checkAndSet("x", Buf.Utf8("z"), casUnique).map(_.replaced)).booleanValue
      )
      val res = awaitResult(client.get("x"))
      assert(res.isDefined)
      assert(res.get == Buf.Utf8("z"))
    }
  }

  test("append & prepend") {
    awaitResult(client.set("foo", Buf.Utf8("bar")))
    awaitResult(client.append("foo", Buf.Utf8("rab")))
    val Buf.Utf8(res) = awaitResult(client.get("foo")).get
    assert(res == "barrab")
    awaitResult(client.prepend("foo", Buf.Utf8("rab")))
    val Buf.Utf8(res2) = awaitResult(client.get("foo")).get
    assert(res2 == "rabbarrab")
  }

  test("incr & decr") {
    // As of memcached 1.4.8 (issue 221), empty values are no longer treated as integers
    awaitResult(client.set("foo", Buf.Utf8("0")))
    assert(awaitResult(client.incr("foo")) == Some(1L))
    assert(awaitResult(client.incr("foo", 2)) == Some(3L))
    assert(awaitResult(client.decr("foo")) == Some(2L))

    awaitResult(client.set("foo", Buf.Utf8("0")))
    assert(awaitResult(client.incr("foo")) == Some(1L))
    val l = 1L << 50
    assert(awaitResult(client.incr("foo", l)) == Some(l + 1L))
    assert(awaitResult(client.decr("foo")) == Some(l))
    assert(awaitResult(client.decr("foo", l)) == Some(0L))
  }

  if (Option(System.getProperty("USE_EXTERNAL_MEMCACHED")).isDefined) {
    test("stats") {
      val stats = awaitResult(client.stats())
      assert(stats != null)
      assert(!stats.isEmpty)
      stats.foreach { stat => assert(stat.startsWith("STAT")) }
    }
  }

  test("send malformed keys") {
    // test key validation trait
    intercept[ClientError] { awaitResult(client.get("fo o")) }
    intercept[ClientError] { awaitResult(client.set("", Buf.Utf8("bar"))) }
    intercept[ClientError] { awaitResult(client.get("    foo")) }
    intercept[ClientError] { awaitResult(client.get("foo   ")) }
    intercept[ClientError] { awaitResult(client.get("    foo")) }
    intercept[ClientError] { awaitResult(client.get(null: String)) }
    intercept[ClientError] { awaitResult(client.set(null: String, Buf.Utf8("bar"))) }
    intercept[ClientError] { awaitResult(client.set("    ", Buf.Utf8("bar"))) }

    try { awaitResult(client.set("\t", Buf.Utf8("bar"))) }
    catch {
      case _: ClientError => fail("\t is allowed")
    }

    intercept[ClientError] { awaitResult(client.set("\r", Buf.Utf8("bar"))) }
    intercept[ClientError] { awaitResult(client.set("\n", Buf.Utf8("bar"))) }
    intercept[ClientError] { awaitResult(client.set("\u0000", Buf.Utf8("bar"))) }

    val veryLongKey =
      "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz"
    intercept[ClientError] { awaitResult(client.get(veryLongKey)) }
    intercept[ClientError] { awaitResult(client.set(veryLongKey, Buf.Utf8("bar"))) }

    // test other keyed command validation
    intercept[ClientError] { awaitResult(client.get(null: Seq[String])) }
    intercept[ClientError] { awaitResult(client.gets(null: Seq[String])) }
    intercept[ClientError] { awaitResult(client.gets(Seq(null))) }
    intercept[ClientError] { awaitResult(client.gets(Seq(""))) }
    intercept[ClientError] { awaitResult(client.gets(Seq("foos", "bad key", "somethingelse"))) }
    intercept[ClientError] { awaitResult(client.append("bad key", Buf.Utf8("rab"))) }
    intercept[ClientError] { awaitResult(client.prepend("bad key", Buf.Utf8("rab"))) }
    intercept[ClientError] { awaitResult(client.replace("bad key", Buf.Utf8("bar"))) }
    intercept[ClientError] { awaitResult(client.add("bad key", Buf.Utf8("2"))) }
    intercept[ClientError] {
      awaitResult(client.checkAndSet("bad key", Buf.Utf8("z"), Buf.Utf8("2")))
    }
    intercept[ClientError] { awaitResult(client.incr("bad key")) }
    intercept[ClientError] { awaitResult(client.decr("bad key")) }
    intercept[ClientError] { awaitResult(client.delete("bad key")) }
  }

  test("Push client uses Netty4PushTransporter") {
    val simple = new SimpleRegistry()
    val address = Address(testServer.get.address.asInstanceOf[InetSocketAddress])
    GlobalRegistry.withRegistry(simple) {
      val client = Memcached.client.newService(Name.bound(address), "memcache")
      client(Quit())
      val entries = simple.toSet
      assert(
        entries.contains(
          Entry(Seq("client", "memcached", "memcache", "Transporter"), "Netty4PushTransporter")
        )
      )
    }
  }

  test("annotates the total number of hits and misses") {
    awaitResult(client.set("foo", Buf.Utf8("bar")))
    awaitResult(client.set("bar", Buf.Utf8("baz")))
    withExpectedTraces(
      c => {
        // add a missing key
        awaitResult(c.getResult(Seq("foo", "bar", "themissingkey")))
      },
      Seq(
        Annotation.BinaryAnnotation("clnt/memcached.hits", 2),
        Annotation.BinaryAnnotation("clnt/memcached.misses", 1)
      )
    )
  }

  test("annotates the shard id of the endpoint") {
    awaitResult(client.set("foo", Buf.Utf8("bar")))
    withExpectedTraces(
      c => {
        awaitResult(c.get("foo"))
      },
      Seq(
        Annotation.BinaryAnnotation("clnt/memcached.shard_id", 0)
      )
    )
  }
}
