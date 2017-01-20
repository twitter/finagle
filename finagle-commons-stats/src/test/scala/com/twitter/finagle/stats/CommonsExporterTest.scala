package com.twitter.finagle.stats

import com.twitter.common.stats.Stats
import com.twitter.conversions.time._
import com.twitter.finagle.http.{Request, HttpMuxer}
import com.twitter.util.{Future, Await}
import org.scalatest.{BeforeAndAfter, FunSuite}

class CommonsExporterTest extends FunSuite with BeforeAndAfter {

  def await[T](t: Future[T]): T = Await.result(t, 5.seconds)

  before {
    Stats.flush
  }

  test("CommonsExporter should be able to export for counters") {
    val sr = new CommonsStatsReceiver()
    val counter = sr.counter("foo")
    counter.incr()

    val exporter = new CommonsExporter()

    val rep = await(exporter(Request()))
    assert(rep.contentString == "{\"foo\":1}")
  }

  test("CommonsExporter should be able to export for gauges") {
    val sr = new CommonsStatsReceiver()
    val gauge = sr.addGauge("bar") { 2f }

    val exporter = new CommonsExporter()

    val rep = await(exporter(Request()))
    assert(rep.contentString == "{\"bar\":2.0}")
  }

  test("CommonsExporter should be able to export for histograms") {
    val sr = new CommonsStatsReceiver()
    val stat = sr.stat("qux")

    // it's an enormous hassle to actually sample these because TimeSeriesRepositoryImpl
    // hasn't been published to maven central.
    // for (i <- 0 until 1000) {
    //   stat.add(i)
    // }

    val exporter = new CommonsExporter()

    val rep = await(exporter(Request()))
    assert(rep.contentString == "{\"qux_50_0_percentile\":0.0,\"qux_95_0_percentile\":0.0,\"qux_99_0_percentile\":0.0}")
  }

  test("CommonsExporter should be able to export stats made directly") {
    val atom = Stats.exportLong("baz", 3L)
    val exporter = new CommonsExporter()
    val rep = await(exporter(Request()))
    assert(rep.contentString == "{\"baz\":3}")
  }

  test("Loaded HttpMuxer") {
    assert(HttpMuxer.patterns.contains("/vars.json"))
  }
}
