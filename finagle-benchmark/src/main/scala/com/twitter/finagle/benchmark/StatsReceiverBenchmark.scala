package com.twitter.finagle.benchmark

import com.google.caliper.SimpleBenchmark
import com.twitter.finagle.stats.{MetricsStatsReceiver, OstrichStatsReceiver, StatsReceiver}
import java.util.Random

// From $BIRDCAGE_HOME run:
// ./bin/caliper finagle/finagle-benchmark com.twitter.finagle.benchmark.StatsReceiverBenchmark
class StatsReceiverBenchmark extends SimpleBenchmark {

  private[this] val rnd = new Random

  private[this] def test(receiver: StatsReceiver, f: Int => Any, n: Int) {
    var i = 0
    while (i < n) {
      var j = 0
      while (j < n) {
        f(rnd.nextInt(n))
        j += 1
      }
      i += 1
    }
  }

  private[this] def testStatsInsertions(receiver: StatsReceiver, n: Int) {
    val stat = receiver.stat("my_stat")
    test(receiver, stat.add(_), n)
  }

  def timeOstrichStatsInsertion(nreps: Int) {
    testStatsInsertions(new OstrichStatsReceiver(), nreps)
  }

  def timeMetricsStatsInsertion(nreps: Int) {
    testStatsInsertions(new MetricsStatsReceiver(), nreps)
  }

  def timeOstrichStatsQuery(n: Int) {
    val receiver = new OstrichStatsReceiver()
    (1 to n) foreach { x => receiver.stat("my_stat").add(rnd.nextInt(x)) }
    var i = 0
    while (i < n) {
      receiver.repr.getMetric("my_stat")().toMap
      i += 1
    }
  }

  def timeMetricsStatsQuery(n: Int) {
    val receiver = new MetricsStatsReceiver()
    (1 to n) foreach { x => receiver.stat("my_stat").add(rnd.nextInt(x)) }
    var i = 0
    while (i < n) {
      receiver.registry.sample()
      i += 1
    }
  }
}
