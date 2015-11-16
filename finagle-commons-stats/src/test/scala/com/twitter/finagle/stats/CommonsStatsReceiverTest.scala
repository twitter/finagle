package com.twitter.finagle.stats

import com.twitter.common.stats.Stats
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfter, OneInstancePerTest, FunSuite, Assertions}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CommonsStatsReceiverTest extends FunSuite with BeforeAndAfter with OneInstancePerTest {

  before {
    Stats.flush
  }

  after {
    Stats.flush
  }

  test("counter should return a new counter object with the given name and reflect incr operations") {
    val counter = (new CommonsStatsReceiver()).counter("foo")
    assert(Stats.getVariable("foo").read.asInstanceOf[Long] == 0)
    counter.incr(7)
    assert(Stats.getVariable("foo").read.asInstanceOf[Long] == 7)
    counter.incr(-8)
    assert(Stats.getVariable("foo").read.asInstanceOf[Long] == -1)
  }

  test("counter should memoize objects with the same name") {
    val sr = new CommonsStatsReceiver()
    assert(sr.counter("one") eq sr.counter("one"))
    assert(sr.counter("one", "two") eq sr.counter("one", "two"))
    assert(sr.counter("one") != sr.counter("one", "two"))
  }

  test("stat should work") {
    val stat = (new CommonsStatsReceiver()).stat("bar")

    assert(Stats.getVariable[Float]("bar_50_0_percentile").read === 0.0f)
    assert(Stats.getVariable[Float]("bar_95_0_percentile").read === 0.0f)
    assert(Stats.getVariable[Float]("bar_99_0_percentile").read === 0.0f)

    for (i <- 0.until(10000)) {
      stat.add(i.toFloat)
    }

    //TODO find a way to poke at the stats, need to do something with a StatsModule
  }

  test("stat should be memoized") {
    val receiver = new CommonsStatsReceiver()
    val stat1 = receiver.stat("what")
    val stat2 = receiver.stat("what")
    assert(stat1 eq stat2)
  }

  test("addGauge should work") {
    @volatile var inner = 9.0f
    // val needed here to add a strong ref to the gauge otherwise it will get collected
    val myGauge = (new CommonsStatsReceiver).addGauge("bam") { inner }
    inner = 1.0f
    assert(Stats.getVariable("bam").read.asInstanceOf[Float] == 1.0f)
    inner = 3.14f
    assert(Stats.getVariable("bam").read.asInstanceOf[Float] == 3.14f)
  }

  test("Loaded receiver") {
    DefaultStatsReceiver.counter("loadedfoo").incr()
    assert(Stats.getVariable("loadedfoo").read.asInstanceOf[Long] == 1)
  }
}
