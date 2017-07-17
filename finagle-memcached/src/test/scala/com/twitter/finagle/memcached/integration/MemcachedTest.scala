package com.twitter.finagle.memcached.integration

import com.twitter.conversions.time._
import com.twitter.finagle._
import com.twitter.finagle.liveness.FailureAccrualFactory
import com.twitter.finagle.memcached.protocol.ClientError
import com.twitter.finagle.memcached.{Client, PartitionedClient}
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.io.Buf
import com.twitter.util._
import java.net.{InetAddress, InetSocketAddress}
import org.scalatest.{BeforeAndAfter, FunSuite, Outcome}

class MemcachedTest extends FunSuite with BeforeAndAfter {

  val NumServers = 5
  val NumConnections = 4

  var servers: Seq[TestMemcachedServer] = Seq.empty
  var client: Client = null

  val TimeOut = 15.seconds

  private def awaitResult[T](awaitable: Awaitable[T]): T = Await.result(awaitable, TimeOut)

  private val clientName = "test_client"
  before {
    val serversOpt = for (_ <- 1 to NumServers) yield TestMemcachedServer.start()

    if (serversOpt.forall(_.isDefined)) {
      servers = serversOpt.flatten
      val n = Name.bound(servers.map { s => (Address(s.address)) }: _*)
      client = Memcached.client.newRichClient(n, clientName)
    }
  }

  after {
    servers.foreach(_.stop())
  }

  override def withFixture(test: NoArgTest): Outcome = {
    if (servers.length == NumServers) test() else {
      info("Cannot start memcached. Skipping test...")
      cancel()
    }
  }

  test("set & get") {
    awaitResult(client.delete("foo"))
    assert(awaitResult(client.get("foo")) == None)
    awaitResult(client.set("foo", Buf.Utf8("bar")))
    assert(awaitResult(client.get("foo")).get == Buf.Utf8("bar"))
  }

  test("set & get data containing newlines") {
    awaitResult(client.delete("bob"))
    assert(awaitResult(client.get("bob")) == None)
    awaitResult(client.set("bob", Buf.Utf8("hello there \r\n nice to meet \r\n you")))
    assert(awaitResult(client.get("bob")).get ==
      Buf.Utf8("hello there \r\n nice to meet \r\n you"), 3.seconds)
  }

  test("get") {
    awaitResult(client.set("foo", Buf.Utf8("bar")))
    awaitResult(client.set("baz", Buf.Utf8("boing")))
    val result =
      awaitResult(
        client.get(Seq("foo", "baz", "notthere"))
      ).map { case (key, Buf.Utf8(value)) =>
        (key, value)
      }
    assert(result == Map("foo" -> "bar", "baz" -> "boing"))
  }

  test("getWithFlag") {
    awaitResult(client.set("foo", Buf.Utf8("bar")))
    awaitResult(client.set("baz", Buf.Utf8("boing")))
    val result = awaitResult(client.getWithFlag(Seq("foo", "baz", "notthere")))
      .map { case (key, ((Buf.Utf8(value), Buf.Utf8(flag)))) => (key, (value, flag)) }
    assert(result == Map(
      "foo" -> (("bar", "0")),
      "baz" -> (("boing", "0"))
    ))
  }

  if (Option(System.getProperty("USE_EXTERNAL_MEMCACHED")).isDefined) {
    test("gets") {
      // create a client that connects to only one server so we can predict CAS tokens
      val client = Memcached.client.newRichClient(
        Name.bound(Address(servers(0).address)), "client")

      awaitResult(client.set("foos", Buf.Utf8("xyz"))) // CAS: 1
      awaitResult(client.set("bazs", Buf.Utf8("xyz"))) // CAS: 2
      awaitResult(client.set("bazs", Buf.Utf8("zyx"))) // CAS: 3
      awaitResult(client.set("bars", Buf.Utf8("xyz"))) // CAS: 4
      awaitResult(client.set("bars", Buf.Utf8("zyx"))) // CAS: 5
      awaitResult(client.set("bars", Buf.Utf8("yxz"))) // CAS: 6
      val result =
        awaitResult(
          client.gets(Seq("foos", "bazs", "bars", "somethingelse"))
        ).map { case (key, (Buf.Utf8(value), Buf.Utf8(casUnique))) =>
          (key, (value, casUnique))
        }
      val expected =
        Map(
          "foos" -> (("xyz", "1")), // the "cas unique" values are predictable from a fresh memcached
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
        .map { case (key, (Buf.Utf8(value), Buf.Utf8(flag), Buf.Utf8(casUnique))) =>
          (key, (value, flag, casUnique))
        }

      assert(result == Map(
        "foos1" -> (("xyz", "0", "1")),  // the "cas unique" values are predictable from a fresh memcached
        "bazs1" -> (("zyx", "0", "2"))
      ))
    }
  }

  if (Option(System.getProperty("USE_EXTERNAL_MEMCACHED")).isDefined) {
    test("cas") {
      awaitResult(client.set("x", Buf.Utf8("y")))
      val Some((value, casUnique)) = awaitResult(client.gets("x"))
      assert(value == Buf.Utf8("y"))
      assert(casUnique == Buf.Utf8("1"))

      assert(!awaitResult(client.checkAndSet("x", Buf.Utf8("z"), Buf.Utf8("2")).map(_.replaced)))
      assert(awaitResult(client.checkAndSet("x", Buf.Utf8("z"), casUnique).map(_.replaced)).booleanValue)
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
    assert(awaitResult(client.incr("foo"))    == Some(1L))
    assert(awaitResult(client.incr("foo", 2)) == Some(3L))
    assert(awaitResult(client.decr("foo"))    == Some(2L))

    awaitResult(client.set("foo", Buf.Utf8("0")))
    assert(awaitResult(client.incr("foo"))    == Some(1L))
    val l = 1L << 50
    assert(awaitResult(client.incr("foo", l)) == Some(l + 1L))
    assert(awaitResult(client.decr("foo"))    == Some(l))
    assert(awaitResult(client.decr("foo", l)) == Some(0L))
  }

  if (Option(System.getProperty("USE_EXTERNAL_MEMCACHED")).isDefined) {
    test("stats") {
      // We can't use a partitioned client to get stats, because we don't hash to a server based on
      // a key. Instead, we create a ConnectedClient, which is connected to one server.
      val service = Memcached.client.newService(Name.bound(Address(servers(0).address)), "client")

      val connectedClient = Client(service)
      val stats = awaitResult(connectedClient.stats())
      assert(stats != null)
      assert(!stats.isEmpty)
      stats.foreach { stat =>
        assert(stat.startsWith("STAT"))
      }
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
    intercept[NullPointerException] { awaitResult(client.get(nullString)) }
    intercept[NullPointerException] { awaitResult(client.set(nullString, Buf.Utf8("bar"))) }
    intercept[ClientError] { awaitResult(client.set("    ", Buf.Utf8("bar"))) }

    assert(awaitResult(client.set("\t", Buf.Utf8("bar")).liftToTry) == Return.Unit) // "\t" is a valid key
    intercept[ClientError] { awaitResult(client.set("\r", Buf.Utf8("bar"))) }
    intercept[ClientError] { awaitResult(client.set("\n", Buf.Utf8("bar"))) }
    intercept[ClientError] { awaitResult(client.set("\u0000", Buf.Utf8("bar"))) }

    val veryLongKey = "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz"
    intercept[ClientError] { awaitResult(client.get(veryLongKey)) }
    intercept[ClientError] { awaitResult(client.set(veryLongKey, Buf.Utf8("bar"))) }

    // test other keyed command validation
    val nullSeq:Seq[String] = null
    intercept[NullPointerException] { awaitResult(client.get(nullSeq)) }
    intercept[ClientError] { awaitResult(client.append("bad key", Buf.Utf8("rab"))) }
    intercept[ClientError] { awaitResult(client.prepend("bad key", Buf.Utf8("rab"))) }
    intercept[ClientError] { awaitResult(client.replace("bad key", Buf.Utf8("bar"))) }
    intercept[ClientError] { awaitResult(client.add("bad key", Buf.Utf8("2"))) }
    intercept[ClientError] { awaitResult(client.checkAndSet("bad key", Buf.Utf8("z"), Buf.Utf8("2"))) }
    intercept[ClientError] { awaitResult(client.incr("bad key")) }
    intercept[ClientError] { awaitResult(client.decr("bad key")) }
    intercept[ClientError] { awaitResult(client.delete("bad key")) }
  }

  test("re-hash when a bad host is ejected") {
    client = Memcached.client
      .configured(FailureAccrualFactory.Param(1, () => 10.minutes))
      .configured(Memcached.param.EjectFailedHost(true))
      .newRichClient(Name.bound(servers.map { s => (Address(s.address)) }: _*), "test_client")
    val partitionedClient = client.asInstanceOf[PartitionedClient]

    // set values
    awaitResult(Future.collect(
      (0 to 20).map { i =>
        client.set(s"foo$i", Buf.Utf8(s"bar$i"))
      }
    ))

    // We can't control the Distributor to make sure that for the set of servers, there is at least
    // one client in the partition talking to it. Therefore, we rely on the fact that for 5
    // backends, it's very unlikely all clients will be talking to the same server, and as such,
    // shutting down all backends but one will trigger cache misses.
    servers.tail.foreach(_.stop)

    // trigger ejection
    for (i <- 0 to 20) {
      Await.ready(client.get(s"foo$i"), TimeOut)
    }

    val clientSet =
      (0 to 20).foldLeft(Set[Client]()){ case (s, i) =>
        val c = partitionedClient.clientOf(s"foo$i")
        s + c
      }
    assert(clientSet.size == 1)

    // previously set values have cache misses
    var cacheMisses = 0
    for (i <- 0 to 20) {
      if (awaitResult(client.get(s"foo$i")) == None) cacheMisses = cacheMisses + 1
    }
    assert(cacheMisses > 0)
  }

  test("host comes back into ring after being ejected") {
    import com.twitter.finagle.memcached.protocol._

    class MockedMemcacheServer extends Service[Command, Response] {
      def apply(command: Command) = command match {
        case Get(key) => Future.value(Values(List(Value(Buf.Utf8("foo"), Buf.Utf8("bar")))))
        case Set(_, _, _, _) => Future.value(Error(new Exception))
        case x => Future.exception(new MatchError(x))
      }
    }

    val cacheServer = Memcached.serve(
      new InetSocketAddress(InetAddress.getLoopbackAddress, 0),
      new MockedMemcacheServer)

    val timer = new MockTimer
    val statsReceiver = new InMemoryStatsReceiver

    val client = Memcached.client
      .configured(FailureAccrualFactory.Param(1, () => 10.minutes))
      .configured(Memcached.param.EjectFailedHost(true))
      .configured(param.Timer(timer))
      .configured(param.Stats(statsReceiver))
      .newRichClient(Name.bound(Address(cacheServer.boundAddress.asInstanceOf[InetSocketAddress])), "cacheClient")

    Time.withCurrentTimeFrozen { timeControl =>

      // Send a bad request
      intercept[Exception] { awaitResult(client.set("foo", Buf.Utf8("bar"))) }

      // Node should have been ejected
      assert(statsReceiver.counters.get(List("cacheClient", "ejections")) == Some(1))

      // Node should have been marked dead, and still be dead after 5 minutes
      timeControl.advance(5.minutes)

      // Shard should be unavailable
      intercept[ShardNotAvailableException] {
        awaitResult(client.get(s"foo"))
      }

      timeControl.advance(5.minutes)
      timer.tick()

      // 10 minutes (markDeadFor duration) have passed, so the request should go through
      assert(statsReceiver.counters.get(List("cacheClient", "revivals")) == Some(1))
      assert(awaitResult(client.get(s"foo")).get == Buf.Utf8("bar"))
    }
  }

  test("Add and remove nodes") {
    val addrs = servers.map { s => (Address(s.address)) }

    // Start with 3 backends
    val mutableAddrs: ReadWriteVar[Addr] = new ReadWriteVar(Addr.Bound(addrs.toSet.drop(2)))

    val sr = new InMemoryStatsReceiver
    val myClient = Memcached.client
      .connectionsPerEndpoint(NumConnections)
      .withStatsReceiver(sr)
      .newRichClient(Name.Bound.singleton(mutableAddrs), "test_client")

    assert(sr.counters(Seq("test_client", "redistributes")) == 1)
    assert(sr.counters(Seq("test_client", "loadbalancer", "rebuilds")) == 3)
    assert(sr.counters(Seq("test_client", "loadbalancer", "updates")) == 3)
    assert(sr.counters(Seq("test_client", "loadbalancer", "adds")) == NumConnections * 3)
    assert(sr.counters(Seq("test_client", "loadbalancer", "removes")) == 0)

    // Add 2 nodes to the backends, for a total of 5 backends
    mutableAddrs.update(Addr.Bound(addrs.toSet))

    assert(sr.counters(Seq("test_client", "redistributes")) == 2)
    // Need to rebuild each of the 5 nodes with `numConnections`
    assert(sr.counters(Seq("test_client", "loadbalancer", "rebuilds")) == 5)
    assert(sr.counters(Seq("test_client", "loadbalancer", "updates")) == 5)
    assert(sr.counters(Seq("test_client", "loadbalancer", "adds")) == NumConnections * 5)
    assert(sr.counters(Seq("test_client", "loadbalancer", "removes")) == 0)

    // Remove 1 node from the backends, for a total of 4 backends
    mutableAddrs.update(Addr.Bound(addrs.toSet.drop(1)))

    assert(sr.counters(Seq("test_client", "redistributes")) == 3)
    // Don't need to rebuild or update any existing nodes
    assert(sr.counters(Seq("test_client", "loadbalancer", "rebuilds")) == 5)
    assert(sr.counters(Seq("test_client", "loadbalancer", "updates")) == 5)
    assert(sr.counters(Seq("test_client", "loadbalancer", "adds")) == NumConnections * 5)
    assert(sr.counters(Seq("test_client", "leaves")) == 1)
    // Node is removed, closing `numConnections` in the LoadBalancer
    assert(sr.counters(Seq("test_client", "loadbalancer", "removes")) == NumConnections)

    // Update the backends with the same list, for a total of 4 backends
    mutableAddrs.update(Addr.Bound(addrs.toSet.drop(1)))

    assert(sr.counters(Seq("test_client", "redistributes")) == 4)
    // Ensure we don't do anything in the LoadBalancer because the set of nodes is the same
    assert(sr.counters(Seq("test_client", "loadbalancer", "rebuilds")) == 5)
    assert(sr.counters(Seq("test_client", "loadbalancer", "updates")) == 5)
    assert(sr.counters(Seq("test_client", "loadbalancer", "adds")) == NumConnections * 5)
    assert(sr.counters(Seq("test_client", "loadbalancer", "removes")) == NumConnections)
  }

  test("FailureAccrualFactoryException has remote address") {

    val client = Memcached.client
      .connectionsPerEndpoint(1)
      // 1 failure triggers FA; make sure FA stays in "dead" state after failure
      .configured(FailureAccrualFactory.Param(1, 10.minutes))
      .withEjectFailedHost(false)
      .newTwemcacheClient(Name.bound(Address("localhost", 1234)), "client")

    // Trigger transition to "Dead" state
    intercept[Exception] {
      awaitResult(client.delete("foo"))
    }

    // Client has not been ejected, so the same client gets a re-application of the connection,
    // triggering the 'failureAccrualEx' in KetamaFailureAccrualFactory
    val failureAccrualEx = intercept[HasRemoteInfo] {
      awaitResult(client.delete("foo"))
    }

    assert(failureAccrualEx.getMessage.contains("Endpoint is marked dead by failureAccrual"))
    assert(failureAccrualEx.getMessage.contains("Downstream Address: localhost/127.0.0.1:1234"))
  }
}
