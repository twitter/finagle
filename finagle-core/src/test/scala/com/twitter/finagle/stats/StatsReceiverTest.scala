package com.twitter.finagle.stats

import com.twitter.conversions.time._
import com.twitter.finagle.builder.{ClientBuilder, ServerBuilder}
import com.twitter.finagle.integration.StringCodec
import com.twitter.finagle.{Service, WriteException, IndividualRequestTimeoutException}
import com.twitter.util.{Await, Future, Promise}
import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import scala.collection.mutable.ArrayBuffer

@RunWith(classOf[JUnitRunner])
class StatsReceiverTest extends FunSuite {
  test("RollupStatsReceiver counter/stats") {
    val mem = new InMemoryStatsReceiver
    val receiver = new RollupStatsReceiver(mem)

    receiver.counter("toto", "titi", "tata").incr()
    assert(mem.counters(Seq("toto")) === 1)
    assert(mem.counters(Seq("toto", "titi")) === 1)
    assert(mem.counters(Seq("toto", "titi", "tata")) === 1)

    receiver.counter("toto", "titi", "tutu").incr()
    assert(mem.counters(Seq("toto")) === 2)
    assert(mem.counters(Seq("toto", "titi")) === 2)
    assert(mem.counters(Seq("toto", "titi", "tata")) === 1)
    assert(mem.counters(Seq("toto", "titi", "tutu")) === 1)
  }

  test("Broadcast Counter/Stat") {
    class MemCounter extends Counter {
      var c = 0
      def incr(delta: Int) { c += delta }
    }
    val c1 = new MemCounter
    val c2 = new MemCounter
    val broadcastCounter = BroadcastCounter(Seq(c1, c2))
    assert(c1.c === 0)
    assert(c2.c === 0)

    broadcastCounter.incr()
    assert(c1.c === 1)
    assert(c2.c === 1)

    class MemStat extends Stat {
      var values: Seq[Float] = ArrayBuffer.empty[Float]
      def add(f: Float) { values = values :+ f }
    }
    val s1 = new MemStat
    val s2 = new MemStat
    val broadcastStat = BroadcastStat(Seq(s1, s2))
    assert(s1.values === Seq.empty)
    assert(s2.values === Seq.empty)

    broadcastStat.add(1F)
    assert(s1.values === Seq(1F))
    assert(s2.values === Seq(1F))
  }

  test("StatsReceiver time") {
    val receiver = spy(new InMemoryStatsReceiver)

    receiver.time("er", "mah", "gerd") { () }
    verify(receiver, times(1)).stat("er", "mah", "gerd")

    receiver.time(TimeUnit.NANOSECONDS, "er", "mah", "gerd") { () }
    verify(receiver, times(2)).stat("er", "mah", "gerd")

    val stat = receiver.stat("er", "mah", "gerd")
    verify(receiver, times(3)).stat("er", "mah", "gerd")

    receiver.time(TimeUnit.DAYS, stat) { () }
    verify(receiver, times(3)).stat("er", "mah", "gerd")
  }

  test("StatsReceiver timeFuture") {
    val receiver = spy(new InMemoryStatsReceiver)

    Await.ready(receiver.timeFuture("2", "chainz") { Future.Unit }, 1.second)
    verify(receiver, times(1)).stat("2", "chainz")

    Await.ready(receiver.timeFuture(TimeUnit.MINUTES, "2", "chainz") { Future.Unit }, 1.second)
    verify(receiver, times(2)).stat("2", "chainz")

    val stat = receiver.stat("2", "chainz")
    verify(receiver, times(3)).stat("2", "chainz")

    Await.result(receiver.timeFuture(TimeUnit.HOURS, stat) { Future.Unit }, 1.second)
    verify(receiver, times(3)).stat("2", "chainz")
  }

  test("StatsReceiver.scope: prefix stats by a scope string") {
    val receiver = new InMemoryStatsReceiver
    val scoped = receiver.scope("foo")
    receiver.counter("bar").incr()
    scoped.counter("baz").incr()

    assert(receiver.counters(Seq("bar")) === 1)
    assert(receiver.counters(Seq("foo", "baz")) === 1)
  }

  test("StatsReceiver.scope: don't prefix with the empty string") {
    val receiver = new InMemoryStatsReceiver
    val scoped = receiver.scope("")
    receiver.counter("bar").incr()
    scoped.counter("baz").incr()

    assert(receiver.counters(Seq("bar")) === 1)
    assert(receiver.counters(Seq("baz")) === 1)
  }

  test("Scoped equality") {
    val sr = new InMemoryStatsReceiver
    assert(sr === sr)
    assert(sr.scope("foo") != sr.scope("bar"))
  }

  test("Scoped forwarding to NullStatsReceiver") {
    assert(NullStatsReceiver.scope("foo").scope("bar").isNull)
  }

  // CSL-1191
  if (!sys.props.contains("SKIP_FLAKY"))

  test("rollup statsReceiver work in action") {
    val never = new Service[String, String] {
      def apply(request: String) = new Promise[String]
    }
    val address = new InetSocketAddress(0)
    val server = ServerBuilder()
      .codec(StringCodec)
      .bindTo(address)
      .name("FinagleServer")
      .build(never)

    val mem = new InMemoryStatsReceiver
    val client = ClientBuilder()
      .name("client")
      .hosts(server.localAddress)
      .codec(StringCodec)
      .requestTimeout(10.millisecond)
      .hostConnectionLimit(1)
      .hostConnectionMaxWaiters(1)
      .reportTo(mem)
      .build()

    // generate com.twitter.finagle.IndividualRequestTimeoutException
    intercept[IndividualRequestTimeoutException] { Await.result(client("hi")) }
    Await.ready(server.close())
    // generate com.twitter.finagle.WriteException$$anon$1
    intercept[WriteException] { Await.result(client("hi")) }

    val requestFailures = mem.counters(Seq("client", "failures"))
    val serviceCreationFailures =
      mem.counters(Seq("client", "service_creation", "failures"))

    assert(requestFailures === 1)
    assert(serviceCreationFailures === 1)
  }

  test("Forwarding to LoadedStatsReceiver") {
    val prev = LoadedStatsReceiver.self
    LoadedStatsReceiver.self = NullStatsReceiver

    val dsr = DefaultStatsReceiver // StatsReceiverProxy
    val csr = ClientStatsReceiver // NameTranslatingStatsReceiver
    val ssr = ServerStatsReceiver // NameTranslatingStatsReceiver

    try {
      assert(dsr.isNull, "DefaultStatsReceiver should be null")
      assert(csr.isNull, "ClientStatsReceiver should be null")
      assert(ssr.isNull, "ServerStatsReceiver should be null")

      val mem = new InMemoryStatsReceiver
      LoadedStatsReceiver.self = mem

      assert(!dsr.isNull, "DefaultStatsReceiver should not be null")
      assert(!csr.isNull, "ClientStatsReceiver should not be null")
      assert(!ssr.isNull, "ServerStatsReceiver should not be null")

      dsr.counter("req").incr()
      csr.counter("req").incr()
      ssr.counter("req").incr()

      assert(mem.counters(Seq("req")) === 1)
      assert(mem.counters(Seq("clnt", "req")) === 1)
      assert(mem.counters(Seq("srv", "req")) === 1)
    } finally {
      LoadedStatsReceiver.self = prev
    }
  }

  test("SummarizingStatsReceiver doesn't fail on empty/low stats") {
    val receiver = new SummarizingStatsReceiver
    assert(receiver.summary() === "# counters\n\n# gauges\n\n# stats\n")

    val stats = receiver.stat("toto")
    stats.add(1)
    val expected = """# counters
      |
      |# gauges
      |
      |# stats
      |toto                           n=1 min=1.0 med=1.0 p90=1.0 p95=1.0 p99=1.0 p999=1.0 p9999=1.0 max=1.0""".stripMargin
    assert(receiver.summary() === expected)

    (2 to 10) foreach { stats.add(_) }
    val expected2 = """# counters
      |
      |# gauges
      |
      |# stats
      |toto                           n=10 min=1.0 med=6.0 p90=10.0 p95=10.0 p99=10.0 p999=10.0 p9999=10.0 max=10.0""".stripMargin
    assert(receiver.summary() === expected2)
  }
}
