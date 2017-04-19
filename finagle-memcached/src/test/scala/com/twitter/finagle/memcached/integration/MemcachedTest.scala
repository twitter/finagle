package com.twitter.finagle.memcached.integration

import com.twitter.conversions.time._
import com.twitter.finagle._
import com.twitter.finagle.liveness.FailureAccrualFactory
import com.twitter.finagle.memcached.protocol.ClientError
import com.twitter.finagle.memcached.{Client, PartitionedClient}
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.io.Buf
import com.twitter.util._
import com.twitter.util.registry.GlobalRegistry
import java.net.{InetAddress, InetSocketAddress}
import org.scalatest.{BeforeAndAfter, FunSuite, Outcome}

class MemcachedTest extends FunSuite with BeforeAndAfter {

  val NumServers = 5
  val NumConnections = 4

  var servers: Seq[TestMemcachedServer] = Seq.empty
  var client: Client = null

  val TimeOut = 15.seconds

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
    Await.result(client.delete("foo"))
    assert(Await.result(client.get("foo")) == None)
    Await.result(client.set("foo", Buf.Utf8("bar")))
    assert(Await.result(client.get("foo")).get == Buf.Utf8("bar"))
  }

  test("set & get data containing newlines") {
    Await.result(client.delete("bob"))
    assert(Await.result(client.get("bob")) == None)
    Await.result(client.set("bob", Buf.Utf8("hello there \r\n nice to meet \r\n you")))
    assert(Await.result(client.get("bob")).get ==
      Buf.Utf8("hello there \r\n nice to meet \r\n you"), 3.seconds)
  }

  test("get") {
    Await.result(client.set("foo", Buf.Utf8("bar")))
    Await.result(client.set("baz", Buf.Utf8("boing")))
    val result =
      Await.result(
        client.get(Seq("foo", "baz", "notthere"))
      ).map { case (key, Buf.Utf8(value)) =>
        (key, value)
      }
    assert(result == Map("foo" -> "bar", "baz" -> "boing"))
  }

  if (Option(System.getProperty("USE_EXTERNAL_MEMCACHED")).isDefined) {
    test("gets") {
      // create a client that connects to only one server so we can predict CAS tokens
      val client = Memcached.client.newRichClient(
        Name.bound(Address(servers(0).address)), "client")

      Await.result(client.set("foos", Buf.Utf8("xyz"))) // CAS: 1
      Await.result(client.set("bazs", Buf.Utf8("xyz"))) // CAS: 2
      Await.result(client.set("bazs", Buf.Utf8("zyx"))) // CAS: 3
      Await.result(client.set("bars", Buf.Utf8("xyz"))) // CAS: 4
      Await.result(client.set("bars", Buf.Utf8("zyx"))) // CAS: 5
      Await.result(client.set("bars", Buf.Utf8("yxz"))) // CAS: 6
      val result =
        Await.result(
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
    test("cas") {
      Await.result(client.set("x", Buf.Utf8("y")))
      val Some((value, casUnique)) = Await.result(client.gets("x"))
      assert(value == Buf.Utf8("y"))
      assert(casUnique == Buf.Utf8("1"))

      assert(!Await.result(client.checkAndSet("x", Buf.Utf8("z"), Buf.Utf8("2")).map(_.replaced)))
      assert(Await.result(client.checkAndSet("x", Buf.Utf8("z"), casUnique).map(_.replaced)).booleanValue)
      val res = Await.result(client.get("x"))
      assert(res.isDefined)
      assert(res.get == Buf.Utf8("z"))
    }
  }

  test("append & prepend") {
    Await.result(client.set("foo", Buf.Utf8("bar")))
    Await.result(client.append("foo", Buf.Utf8("rab")))
    assert(Await.result(client.get("foo")).get == Buf.Utf8("barrab"))
    Await.result(client.prepend("foo", Buf.Utf8("rab")))
    assert(Await.result(client.get("foo")).get == Buf.Utf8("rabbarrab"))
  }

  test("incr & decr") {
    // As of memcached 1.4.8 (issue 221), empty values are no longer treated as integers
    Await.result(client.set("foo", Buf.Utf8("0")))
    assert(Await.result(client.incr("foo"))    == Some(1L))
    assert(Await.result(client.incr("foo", 2)) == Some(3L))
    assert(Await.result(client.decr("foo"))    == Some(2L))

    Await.result(client.set("foo", Buf.Utf8("0")))
    assert(Await.result(client.incr("foo"))    == Some(1L))
    val l = 1L << 50
    assert(Await.result(client.incr("foo", l)) == Some(l + 1L))
    assert(Await.result(client.decr("foo"))    == Some(l))
    assert(Await.result(client.decr("foo", l)) == Some(0L))
  }

  if (Option(System.getProperty("USE_EXTERNAL_MEMCACHED")).isDefined) {
    test("stats") {
      // We can't use a partitioned client to get stats, because we don't hash to a server based on
      // a key. Instead, we create a ConnectedClient, which is connected to one server.
      val service = Memcached.client.newService(Name.bound(Address(servers(0).address)), "client")

      val connectedClient = Client(service)
      val stats = Await.result(connectedClient.stats())
      assert(stats != null)
      assert(!stats.isEmpty)
      stats.foreach { stat =>
        assert(stat.startsWith("STAT"))
      }
    }
  }

  test("send malformed keys") {
    // test key validation trait
    intercept[ClientError] { Await.result(client.get("fo o")) }
    intercept[ClientError] { Await.result(client.set("", Buf.Utf8("bar"))) }
    intercept[ClientError] { Await.result(client.get("    foo")) }
    intercept[ClientError] { Await.result(client.get("foo   ")) }
    intercept[ClientError] { Await.result(client.get("    foo")) }
    val nullString: String = null
    intercept[NullPointerException] { Await.result(client.get(nullString)) }
    intercept[NullPointerException] { Await.result(client.set(nullString, Buf.Utf8("bar"))) }
    intercept[ClientError] { Await.result(client.set("    ", Buf.Utf8("bar"))) }

    assert(Await.result(client.set("\t", Buf.Utf8("bar")).liftToTry) == Return.Unit) // "\t" is a valid key
    intercept[ClientError] { Await.result(client.set("\r", Buf.Utf8("bar"))) }
    intercept[ClientError] { Await.result(client.set("\n", Buf.Utf8("bar"))) }
    intercept[ClientError] { Await.result(client.set("\u0000", Buf.Utf8("bar"))) }

    val veryLongKey = "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz"
    intercept[ClientError] { Await.result(client.get(veryLongKey)) }
    intercept[ClientError] { Await.result(client.set(veryLongKey, Buf.Utf8("bar"))) }

    // test other keyed command validation
    val nullSeq:Seq[String] = null
    intercept[NullPointerException] { Await.result(client.get(nullSeq)) }
    intercept[ClientError] { Await.result(client.append("bad key", Buf.Utf8("rab"))) }
    intercept[ClientError] { Await.result(client.prepend("bad key", Buf.Utf8("rab"))) }
    intercept[ClientError] { Await.result(client.replace("bad key", Buf.Utf8("bar"))) }
    intercept[ClientError] { Await.result(client.add("bad key", Buf.Utf8("2"))) }
    intercept[ClientError] { Await.result(client.checkAndSet("bad key", Buf.Utf8("z"), Buf.Utf8("2"))) }
    intercept[ClientError] { Await.result(client.incr("bad key")) }
    intercept[ClientError] { Await.result(client.decr("bad key")) }
    intercept[ClientError] { Await.result(client.delete("bad key")) }
  }

  test("re-hash when a bad host is ejected") {
    client = Memcached.client
      .configured(FailureAccrualFactory.Param(1, () => 10.minutes))
      .configured(Memcached.param.EjectFailedHost(true))
      .newRichClient(Name.bound(servers.map { s => (Address(s.address)) }: _*), "test_client")
    val partitionedClient = client.asInstanceOf[PartitionedClient]

    // set values
    Await.result(Future.collect(
      (0 to 20).map { i =>
        client.set(s"foo$i", Buf.Utf8(s"bar$i"))
      }
    ), TimeOut)

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
      if (Await.result(client.get(s"foo$i"), TimeOut) == None) cacheMisses = cacheMisses + 1
    }
    assert(cacheMisses > 0)
  }

  test("GlobalRegistry pipelined client") {
    val expectedKey = Seq("client", "memcached", clientName, "is_pipelining")
    val isPipelining = GlobalRegistry.get.iterator.exists { e =>
      e.key == expectedKey && e.value == "true"
    }
    assert(isPipelining)
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
      intercept[Exception] { Await.result(client.set("foo", Buf.Utf8("bar"))) }

      // Node should have been ejected
      assert(statsReceiver.counters.get(List("cacheClient", "ejections")) == Some(1))

      // Node should have been marked dead, and still be dead after 5 minutes
      timeControl.advance(5.minutes)

      // Shard should be unavailable
      intercept[ShardNotAvailableException] {
        Await.result(client.get(s"foo"))
      }

      timeControl.advance(5.minutes)
      timer.tick()

      // 10 minutes (markDeadFor duration) have passed, so the request should go through
      assert(statsReceiver.counters.get(List("cacheClient", "revivals")) == Some(1))
      assert(Await.result(client.get(s"foo")).get == Buf.Utf8("bar"))
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
      Await.result(client.delete("foo"), 1.second)
    }

    // Client has not been ejected, so the same client gets a re-application of the connection,
    // triggering the 'failureAccrualEx' in KetamaFailureAccrualFactory
    val failureAccrualEx = intercept[HasRemoteInfo] {
      Await.result(client.delete("foo"), 1.second)
    }

    assert(failureAccrualEx.getMessage.contains("Endpoint is marked dead by failureAccrual"))
    assert(failureAccrualEx.getMessage.contains("Downstream Address: localhost/127.0.0.1:1234"))
  }
}
