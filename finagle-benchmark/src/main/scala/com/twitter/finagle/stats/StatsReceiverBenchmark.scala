package com.twitter.finagle.stats

import com.twitter.common.metrics.Metrics
import com.twitter.finagle.stats.{Stat, MetricsStatsReceiver, OstrichStatsReceiver, StatsReceiver}
import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations._
import scala.util.Random

@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Array(Mode.AverageTime))
class StatsReceiverBenchmark {
  import StatsReceiverBenchmark._

  @Benchmark
  def ostrichTimeStatsCreation(state: StatsReceiverState) {
    import state._
    ostrichStatsReceiver.stat("stats_receiver_histogram")
  }

  @Benchmark
  def ostrichTimeStatsInsertion(state: StatState) {
    import state._
    ostrichStat.add(0)
  }

  @Benchmark
  def ostrichTimeStatsQuery(state: QueryState) {
    import state._
    ostrichQuery()
  }

  @Benchmark
  def metricsTimeStatsCreation(state: StatsReceiverState) {
    import state._
    metricsStatsReceiver.stat("stats_receiver_histogram")
  }

  @Benchmark
  def metricsTimeStatsInsertion(state: StatState) {
    import state._
    metricsStat.add(0)
  }

  @Benchmark
  def metricsTimeStatsQuery(state: QueryState) {
    import state._
    metricsQuery()
  }
}

object StatsReceiverBenchmark {
  private[this] val ostrich = new OstrichStatsReceiver
  private[this] val metrics = new MetricsStatsReceiver(Metrics.createDetached())

  @State(Scope.Benchmark)
  class StatsReceiverState {
    val ostrichStatsReceiver: StatsReceiver = ostrich
    val metricsStatsReceiver: StatsReceiver = metrics
  }

  @State(Scope.Benchmark)
  class StatState {
    val ostrichStat: Stat = ostrich.stat("histo")
    val metricsStat: Stat = metrics.stat("histo")
  }

  @State(Scope.Benchmark)
  class QueryState {
    var statName: String = ""
    val rng = new Random(31415926535897932L)

    def ostrichQuery(): Unit = ostrich.repr.get()
    def metricsQuery(): Unit = metrics.registry.sample()

    @Setup(Level.Trial)
    def setup() {
      (1 to 100).foreach { _ =>
        val oStat = ostrich.stat("my_stat")
        val mStat = metrics.stat("my_stat")
        (1 to 1000).foreach { x =>
          val rand = rng.nextInt(x)
          oStat.add(rand)
          mStat.add(rand)
        }
      }
    }

    @TearDown(Level.Trial)
    def teardown() {
      ostrich.repr.clearAll()
    }
  }
}
