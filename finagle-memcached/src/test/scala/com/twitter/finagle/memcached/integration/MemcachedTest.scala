package com.twitter.finagle.memcached.integration

import com.twitter.conversions.DurationOps._
import com.twitter.finagle._
import com.twitter.finagle.memcached.integration.external.TestMemcachedServer
import com.twitter.finagle.memcached.protocol.ClientError
import com.twitter.finagle.memcached.Client
import com.twitter.finagle.memcached.GetResult
import com.twitter.finagle.memcached.GetsResult
import com.twitter.finagle.memcached.PartitionedClient
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.io.Buf
import com.twitter.util._
import java.net.InetAddress
import java.net.InetSocketAddress
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.PatienceConfiguration
import org.scalatest.time.Milliseconds
import org.scalatest.time.Seconds
import org.scalatest.time.Span
import org.scalatest.BeforeAndAfter
import org.scalatest.Outcome
import scala.util.Random
import org.scalatest.funsuite.AnyFunSuite

abstract class MemcachedTest
    extends AnyFunSuite
    with BeforeAndAfter
    with Eventually
    with PatienceConfiguration {

  private val ValueSuffix = ":" + Time.now.inSeconds

  private def randomString(length: Int): String = {
    Random.alphanumeric.take(length).mkString
  }

  protected[this] val NumServers = 5
  protected[this] val NumConnections = 4
  protected[this] val Timeout: Duration = 15.seconds
  protected[this] var servers: Seq[TestMemcachedServer] = Seq.empty
  protected[this] var client: Client = _
  protected[this] val clientName = "test_client"

  protected[this] val redistributesKey: Seq[String]
  protected[this] val leavesKey: Seq[String]
  protected[this] val revivalsKey: Seq[String]
  protected[this] val ejectionsKey: Seq[String]
  protected[this] def createClient(dest: Name, clientName: String): Client

  before {
    val serversOpt = for (_ <- 1 to NumServers) yield TestMemcachedServer.start()

    if (serversOpt.forall(_.isDefined)) {
      servers = serversOpt.flatten
      val dest = Name.bound(servers.map { s => Address(s.address) }: _*)
      client = createClient(dest, clientName)
    }
  }

  after {
    servers.foreach(_.stop())
    client.close()
  }

  override def withFixture(test: NoArgTest): Outcome = {
    if (servers.length == NumServers) {
      test()
    } else {
      info("Cannot start memcached. Skipping test...")
      cancel()
    }
  }

  protected[this] def awaitResult[T](awaitable: Awaitable[T]): T = Await.result(awaitable, Timeout)

  test("set & get") {
    awaitResult(client.delete("foo"))
    assert(awaitResult(client.get("foo")) == None)
    awaitResult(client.set("foo", Buf.Utf8("bar")))
    assert(awaitResult(client.get("foo")).get == Buf.Utf8("bar"))
  }

  test("set and get without partioning") {
    val c = Memcached.client
      .newLoadBalancedTwemcacheClient(
        Name.bound(servers.map { s => Address(s.address) }.head),
        clientName)
    awaitResult(c.set("xyz", Buf.Utf8("value1")))
    assert(awaitResult(c.get("xyz")).get == Buf.Utf8("value1"))
  }

  test("set & get data containing newlines") {
    awaitResult(client.delete("bob"))
    assert(awaitResult(client.get("bob")) == None)
    awaitResult(client.set("bob", Buf.Utf8("hello there \r\n nice to meet \r\n you")))
    assert(
      awaitResult(client.get("bob")).get == Buf.Utf8("hello there \r\n nice to meet \r\n you"),
      3.seconds
    )
  }

  test("empty key sequence") {
    assert(awaitResult(client.get(Seq.empty)).isEmpty)
    assert(awaitResult(client.gets(Seq.empty)).isEmpty)
    assert(awaitResult(client.getWithFlag(Seq.empty)).isEmpty)
    assert(awaitResult(client.getsWithFlag(Seq.empty)).isEmpty)
    assert(awaitResult(client.getResult(Seq.empty)) == GetResult.Empty)
    assert(awaitResult(client.getsResult(Seq.empty)) == GetsResult(GetResult.Empty))
  }

  test("get") {
    awaitResult(client.set("foo", Buf.Utf8("bar")))
    awaitResult(client.set("baz", Buf.Utf8("boing")))
    val result =
      awaitResult(
        client.get(Seq("foo", "baz", "notthere"))
      ).map {
        case (key, Buf.Utf8(value)) =>
          (key, value)
      }
    assert(result == Map("foo" -> "bar", "baz" -> "boing"))
  }

  test("getWithFlag") {
    awaitResult(client.set("foo", Buf.Utf8("bar")))
    awaitResult(client.set("baz", Buf.Utf8("boing")))
    val result = awaitResult(client.getWithFlag(Seq("foo", "baz", "notthere")))
      .map { case (key, ((Buf.Utf8(value), Buf.Utf8(flag)))) => (key, (value, flag)) }
    assert(
      result == Map(
        "foo" -> (("bar", "0")),
        "baz" -> (("boing", "0"))
      )
    )
  }

  if (Option(System.getProperty("USE_EXTERNAL_MEMCACHED")).isDefined) {
    test("gets") {
      // create a client that connects to only one server so we can predict CAS tokens
      awaitResult(client.set("foos", Buf.Utf8("xyz"))) // CAS: 1
      awaitResult(client.set("bazs", Buf.Utf8("xyz"))) // CAS: 2
      awaitResult(client.set("bazs", Buf.Utf8("zyx"))) // CAS: 3
      awaitResult(client.set("bars", Buf.Utf8("xyz"))) // CAS: 4
      awaitResult(client.set("bars", Buf.Utf8("zyx"))) // CAS: 5
      awaitResult(client.set("bars", Buf.Utf8("yxz"))) // CAS: 6
      val result =
        awaitResult(
          client.gets(Seq("foos", "bazs", "bars", "somethingelse"))
        ).map {
          case (key, (Buf.Utf8(value), Buf.Utf8(casUnique))) =>
            (key, (value, casUnique))
        }
      // the "cas unique" values are predictable from a fresh memcached
      val expected =
        Map(
          "foos" -> (("xyz", "1")),
          "bazs" -> (("zyx", "3")),
          "bars" -> (("yxz", "6"))
        )
      assert(result == expected)
    }
  }

  if (Option(System.getProperty("USE_EXTERNAL_MEMCACHED")).isDefined) {
    test("getsWithFlag") {
      awaitResult(client.set("foos1", Buf.Utf8("xyz")))
      awaitResult(client.set("bazs1", Buf.Utf8("xyz")))
      awaitResult(client.set("bazs1", Buf.Utf8("zyx")))
      val result = awaitResult(client.getsWithFlag(Seq("foos1", "bazs1", "somethingelse")))
        .map {
          case (key, (Buf.Utf8(value), Buf.Utf8(flag), Buf.Utf8(casUnique))) =>
            (key, (value, flag, casUnique))
        }

      // the "cas unique" values are predictable from a fresh memcached
      assert(
        result == Map(
          "foos1" -> (("xyz", "0", "1")),
          "bazs1" -> (("zyx", "0", "2"))
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
    assert(awaitResult(client.get("foo")).get == Buf.Utf8("barrab"))
    awaitResult(client.prepend("foo", Buf.Utf8("rab")))
    assert(awaitResult(client.get("foo")).get == Buf.Utf8("rabbarrab"))
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
      // We can't use a partitioned client to get stats, because we don't hash to a server based on
      // a key. Instead, we create a ConnectedClient, which is connected to one server.
      val service = Memcached.client.newService(Name.bound(Address(servers.head.address)), "client")

      val connectedClient = Client(service)
      val stats = awaitResult(connectedClient.stats())
      assert(stats != null)
      assert(stats.nonEmpty)
      stats.foreach { stat => assert(stat.startsWith("STAT")) }
      service.close()
    }
  }

  test("send malformed keys") {
    // test key validation trait
    intercept[ClientError] { awaitResult(client.get("fo o")) }
    intercept[ClientError] { awaitResult(client.set("", Buf.Utf8("bar"))) }
    intercept[ClientError] { awaitResult(client.get("    foo")) }
    intercept[ClientError] { awaitResult(client.get("foo   ")) }
    intercept[ClientError] { awaitResult(client.get("    foo")) }
    val nullString: String = null

    intercept[ClientError] { awaitResult(client.get(nullString)) }
    intercept[ClientError] { awaitResult(client.set(nullString, Buf.Utf8("bar"))) }
    intercept[ClientError] { awaitResult(client.set("    ", Buf.Utf8("bar"))) }

    // "\t" is a valid key
    assert(awaitResult(client.set("\t", Buf.Utf8("bar")).liftToTry) == Return.Unit)
    intercept[ClientError] { awaitResult(client.set("\r", Buf.Utf8("bar"))) }
    intercept[ClientError] { awaitResult(client.set("\n", Buf.Utf8("bar"))) }
    intercept[ClientError] { awaitResult(client.set("\u0000", Buf.Utf8("bar"))) }

    val veryLongKey = "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstu" +
      "vwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdef" +
      "ghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopq" +
      "rstuvwxyz"
    intercept[ClientError] { awaitResult(client.get(veryLongKey)) }
    intercept[ClientError] { awaitResult(client.set(veryLongKey, Buf.Utf8("bar"))) }

    // test other keyed command validation
    val nullSeq: Seq[String] = null
    intercept[NullPointerException] { awaitResult(client.get(nullSeq)) }

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

  protected[this] def testRehashUponEject(client: Client, sr: InMemoryStatsReceiver): Unit = {
    val max = 200
    // set values
    awaitResult(
      Future.collect(
        (0 to max).map { i => client.set(s"foo$i", Buf.Utf8(s"bar$i")) }
      )
    )

    // We can't control the Distributor to make sure that for the set of servers, there is at least
    // one client in the partition talking to it. Therefore, we rely on the fact that for 5
    // backends, it's very unlikely all clients will be talking to the same server, and as such,
    // shutting down all backends but one will trigger cache misses.
    servers.tail.foreach(_.stop())

    // trigger ejection
    for (i <- 0 to max) {
      Await.ready(client.get(s"foo$i"), Timeout)
    }
    // wait a little longer than default to prevent test flappiness
    val timeout = PatienceConfiguration.Timeout(Span(10, Seconds))
    val interval = PatienceConfiguration.Interval(Span(100, Milliseconds))
    eventually(timeout, interval) {
      assert(sr.counters.getOrElse(ejectionsKey, 0L) > 2)
    }

    client match {
      case partitionedClient: PartitionedClient =>
        val clientSet =
          (0 to max).foldLeft(Set[Client]()) {
            case (s, i) =>
              val c = partitionedClient.clientOf(s"foo$i")
              s + c
          }
        assert(clientSet.size == 1)
      case _ =>
      // the new client doesn't have a way to get to the equivalent functionality in
      // ConsistentHashPartitioningService.partitionByKey
    }

    // previously set values have cache misses
    var cacheMisses = 0
    for (i <- 0 to max) {
      if (awaitResult(client.get(s"foo$i")).isEmpty) cacheMisses = cacheMisses + 1
    }
    assert(cacheMisses > 0)
  }

  protected[this] def testRingReEntryAfterEjection(
    createClient: (MockTimer, ListeningServer, StatsReceiver) => Client
  ): Unit = {
    import com.twitter.finagle.memcached.protocol._

    class MockedMemcacheServer extends Service[Command, Response] {
      def apply(command: Command): Future[Response with Product with Serializable] = command match {
        case Get(_) => Future.value(Values(List(Value(Buf.Utf8("foo"), Buf.Utf8("bar")))))
        case Set(_, _, _, _) => Future.value(Error(new Exception))
        case x => Future.exception(new MatchError(x))
      }
    }

    val cacheServer: ListeningServer = Memcached.serve(
      new InetSocketAddress(InetAddress.getLoopbackAddress, 0),
      new MockedMemcacheServer
    )

    val timer = new MockTimer
    val statsReceiver = new InMemoryStatsReceiver

    val client = createClient(timer, cacheServer, statsReceiver)

    Time.withCurrentTimeFrozen { timeControl =>
      // Send a bad request
      intercept[Exception] { awaitResult(client.set("foo", Buf.Utf8("bar"))) }

      // Node should have been ejected
      assert(statsReceiver.counters.get(ejectionsKey) == Some(1))

      // Node should have been marked dead, and still be dead after 5 minutes
      timeControl.advance(5.minutes)

      // Shard should be unavailable
      intercept[ShardNotAvailableException] {
        awaitResult(client.get(s"foo"))
      }

      timeControl.advance(5.minutes)
      timer.tick()

      // 10 minutes (markDeadFor duration) have passed, so the request should go through
      assert(statsReceiver.counters.get(revivalsKey) == Some(1))
      assert(awaitResult(client.get(s"foo")).get == Buf.Utf8("bar"))
    }
    client.close()
  }

  protected[this] def testAddAndRemoveNodes(
    addrs: Seq[Address],
    mutableAddrs: ReadWriteVar[Addr],
    sr: InMemoryStatsReceiver
  ): Unit = {
    assert(sr.counters(redistributesKey) == 1)
    assert(sr.counters(Seq("test_client", "loadbalancer", "rebuilds")) == 3)
    assert(sr.counters(Seq("test_client", "loadbalancer", "updates")) == 3)
    assert(sr.counters(Seq("test_client", "loadbalancer", "adds")) == NumConnections * 3)
    assert(sr.counters(Seq("test_client", "loadbalancer", "removes")) == 0)

    // Add 2 nodes to the backends, for a total of 5 backends
    mutableAddrs.update(Addr.Bound(addrs.toSet))

    assert(sr.counters(redistributesKey) == 2)
    // Need to rebuild each of the 5 nodes with `numConnections`
    assert(sr.counters(Seq("test_client", "loadbalancer", "rebuilds")) == 5)
    assert(sr.counters(Seq("test_client", "loadbalancer", "updates")) == 5)
    assert(sr.counters(Seq("test_client", "loadbalancer", "adds")) == NumConnections * 5)
    assert(sr.counters(Seq("test_client", "loadbalancer", "removes")) == 0)

    // Remove 1 node from the backends, for a total of 4 backends
    mutableAddrs.update(Addr.Bound(addrs.toSet.drop(1)))

    assert(sr.counters(redistributesKey) == 3)
    // Don't need to rebuild or update any existing nodes
    assert(sr.counters(Seq("test_client", "loadbalancer", "rebuilds")) == 5)
    assert(sr.counters(Seq("test_client", "loadbalancer", "updates")) == 5)
    assert(sr.counters(Seq("test_client", "loadbalancer", "adds")) == NumConnections * 5)

    assert(sr.counters(leavesKey) == 1)

    // Node is removed, closing `numConnections` in the LoadBalancer
    assert(sr.counters(Seq("test_client", "loadbalancer", "removes")) == NumConnections)

    // Update the backends with the same list, for a total of 4 backends
    mutableAddrs.update(Addr.Bound(addrs.toSet.drop(1)))

    assert(sr.counters(redistributesKey) == 4)
    // Ensure we don't do anything in the LoadBalancer because the set of nodes is the same
    assert(sr.counters(Seq("test_client", "loadbalancer", "rebuilds")) == 5)
    assert(sr.counters(Seq("test_client", "loadbalancer", "updates")) == 5)
    assert(sr.counters(Seq("test_client", "loadbalancer", "adds")) == NumConnections * 5)
    assert(sr.counters(Seq("test_client", "loadbalancer", "removes")) == NumConnections)
  }

  protected[this] def testFailureAccrualFactoryExceptionHasRemoteAddress(client: Client): Unit = {
    // Trigger transition to "Dead" state
    intercept[Exception] {
      awaitResult(client.delete("foo"))
    }

    // Client has not been ejected, so the same client gets a re-application of the connection,
    // triggering the 'failureAccrualEx' in KetamaFailureAccrualFactory
    val failureAccrualEx = intercept[HasRemoteInfo] {
      awaitResult(client.delete("foo"))
    }

    assert(failureAccrualEx.getMessage().contains("Endpoint is marked dead by failureAccrual"))
    assert(failureAccrualEx.getMessage().contains("Downstream Address: localhost/127.0.0.1:1234"))
  }

  def writeKeys(client: Client, numKeys: Int, keyLength: Int): Seq[String] = {
    // creating multiple random strings so that we get a uniform distribution of keys the
    // ketama ring and thus the Memcached shards
    val keys = 1 to numKeys map { _ => randomString(keyLength) }
    val writes = keys map { key => client.set(key, Buf.Utf8(s"$key$ValueSuffix")) }
    awaitResult(Future.join(writes))
    keys
  }

  def assertRead(client: Client, keys: Seq[String]): Unit = {
    val readValues: Map[String, Buf] = awaitResult { client.get(keys) }
    assert(readValues.size == keys.length)
    assert(readValues.keySet.toSeq.sorted == keys.sorted)
    readValues.keys foreach { key =>
      val Buf.Utf8(readValue) = readValues(key)
      assert(readValue == s"$key$ValueSuffix")
    }
  }

  /**
   * Test compatibility between old and new clients for the migration phase
   */
  protected[this] def testCompatibility(): Unit = {
    val numKeys = 20
    val keyLength = 50

    val newClient = client
    val oldClient = {
      val dest = Name.bound(servers.map { s => Address(s.address) }: _*)
      Memcached.client.newRichClient(dest, clientName)
    }

    // make sure old and new client can read the values written by old client
    val keys1 = writeKeys(oldClient, numKeys, keyLength)
    assertRead(oldClient, keys1)
    assertRead(newClient, keys1)

    // make sure old and new client can read the values written by new client
    val keys2 = writeKeys(newClient, numKeys, keyLength)
    assertRead(oldClient, keys2)
    assertRead(newClient, keys2)
  }

  test("partial success") {
    val keys = writeKeys(client, 1000, 20)
    assertRead(client, keys)

    val initialResult = awaitResult { client.getResult(keys) }
    assert(initialResult.failures.isEmpty)
    assert(initialResult.misses.isEmpty)
    assert(initialResult.values.size == keys.size)

    // now kill one server
    servers.head.stop()

    // test partial success with getResult()
    val getResult = awaitResult { client.getResult(keys) }
    // assert the failures are set to the exception received from the failing partition
    assert(getResult.failures.nonEmpty)
    getResult.failures.foreach {
      case (_, e) =>
        assert(e.isInstanceOf[Exception])
    }
    // there should be no misses as all keys are known
    assert(getResult.misses.isEmpty)

    // assert that the values are what we expect them to be. We are not checking for exact
    // number of failures and successes here because we don't know how many keys will fall into
    // the failed partition. The accuracy of the responses are tested in other tests anyways.
    assert(getResult.values.nonEmpty)
    assert(getResult.values.size < keys.size)
    getResult.values.foreach {
      case (keyStr, valueBuf) =>
        val Buf.Utf8(valStr) = valueBuf
        assert(valStr == s"$keyStr$ValueSuffix")
    }

  }
}
