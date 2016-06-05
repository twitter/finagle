package com.twitter.finagle.stats

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class LoadedStatsReceiverTest extends FunSuite {
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

      assert(mem.counters(Seq("req")) == 1)
      assert(mem.counters(Seq("clnt", "req")) == 1)
      assert(mem.counters(Seq("srv", "req")) == 1)
    } finally {
      LoadedStatsReceiver.self = prev
    }
  }
}
