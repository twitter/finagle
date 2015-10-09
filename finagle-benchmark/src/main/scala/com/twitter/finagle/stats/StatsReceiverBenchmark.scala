package com.twitter.finagle.stats

import com.twitter.common.metrics.{Histogram, Metrics}
import com.twitter.common.stats.{Stats, Stat => CStat}
import com.twitter.finagle.benchmark.StdBenchAnnotations
import com.twitter.ostrich.stats.StatsSummary
import com.twitter.util.events.Sink
import java.util
import org.openjdk.jmh.annotations._
import scala.util.Random

// ./sbt 'project finagle-benchmark' 'run .*StatsReceiverBenchmark.*'
@Threads(3)
class StatsReceiverBenchmark extends StdBenchAnnotations {
  import StatsReceiverBenchmark._

  private[this] def newStat(statRecv: StatsReceiver): Stat =
    statRecv.stat("stats_receiver_histogram")

  private[this] def add(addState: AddState, stat: Stat): Unit = {
    val i = addState.i
    addState.i += 1
    stat.add(i)
  }

  // ----- Ostrich ------

  @Benchmark
  def newStatOstrich(state: StatsReceiverState): Stat =
    newStat(state.ostrichStatsReceiver)

  @Benchmark
  def addOstrich(addState: AddState, state: StatState): Unit =
    add(addState, state.ostrichStat)

  @Benchmark
  def incrOstrich(state: CounterState): Unit =
    state.ostrichCounter.incr()

  @Benchmark
  def queryOstrich(state: QueryState): StatsSummary =
    state.ostrichGet()

  // ----- Commons Metrics ------

  @Benchmark
  def newStatMetricsCommons(state: StatsReceiverState): Stat =
    newStat(state.metricsStatsReceiver)

  @Benchmark
  def addMetricsCommons(addState: AddState, state: StatState): Unit =
    add(addState, state.metricsStat)

  @Benchmark
  def incrMetricsCommons(state: CounterState): Unit =
    state.metricsCounter.incr()

  @Benchmark
  def queryMetricsCommons(state: QueryState): util.Map[String, Number] =
    state.metricsGet()

  // ----- Commons Metrics (Bucketed) ------

  @Benchmark
  def newStatMetricsBucketed(state: StatsReceiverState): Stat =
    newStat(state.metricsBucketedStatsReceiver)

  @Benchmark
  def addMetricsBucketed(addState: AddState, state: StatState): Unit =
    add(addState, state.metricsBucketedStat)

  @Benchmark
  def queryMetricsBucketed(state: QueryState): util.Map[String, Number] =
    state.metricsBucketedGet()

  // ----- Commons Stats ------

  @Benchmark
  def addStatsCommons(addState: AddState, state: StatState): Unit =
    add(addState, state.statsStat)

  @Benchmark
  def incrStatsCommons(state: CounterState): Unit =
    state.statsCounter.incr()

  @Benchmark
  def queryStatsCommons(state: QueryState): java.lang.Iterable[CStat[_]] =
    state.statsGet()
}

object StatsReceiverBenchmark {
  private[this] val ostrich = new OstrichStatsReceiver

  private[this] val metrics = new MetricsStatsReceiver(
    Metrics.createDetached(),
    Sink.default,
    (n: String) => new Histogram(n))

  private[this] val metricsBucketed = new MetricsStatsReceiver(
    Metrics.createDetached(),
    Sink.default,
    (n: String) => new MetricsBucketedHistogram(n))

  private[this] val stats = new CommonsStatsReceiver

  @State(Scope.Benchmark)
  class StatsReceiverState {
    val ostrichStatsReceiver: StatsReceiver = ostrich
    val metricsStatsReceiver: StatsReceiver = metrics
    val metricsBucketedStatsReceiver: StatsReceiver = metricsBucketed
  }

  @State(Scope.Benchmark)
  class StatState {
    val ostrichStat: Stat = ostrich.stat("histo")
    val metricsStat: Stat = metrics.stat("histo")
    val metricsBucketedStat: Stat = metricsBucketed.stat("histo")
    val statsStat: Stat = stats.stat("histo")
  }

  @State(Scope.Benchmark)
  class CounterState {
    val ostrichCounter: Counter = ostrich.counter("cnt")
    val metricsCounter: Counter = metrics.counter("cnt")
    val statsCounter: Counter = stats.counter("cnt")
  }

  @State(Scope.Thread)
  class AddState {
    var i = 0
  }

  @State(Scope.Benchmark)
  class QueryState {
    var statName: String = ""
    val rng = new Random(31415926535897932L)

    def ostrichGet(): StatsSummary = ostrich.repr.get()
    def metricsGet(): util.Map[String, Number] = metrics.registry.sample()
    def metricsBucketedGet(): util.Map[String, Number] = metricsBucketed.registry.sample()
    def statsGet(): java.lang.Iterable[CStat[_]] = Stats.getVariables()

    @Setup(Level.Trial)
    def setup(): Unit = {
      val oStat = ostrich.stat("my_stat")
      val mStat = metrics.stat("my_stat")
      val mbStat = metricsBucketed.stat("my_stat")
      val sStat = stats.stat("my_stat")
      (1 to 100000).foreach { x =>
        val rand = rng.nextInt(x)
        oStat.add(rand)
        mStat.add(rand)
        mbStat.add(rand)
        sStat.add(rand)
      }
    }

    @TearDown(Level.Trial)
    def teardown(): Unit = {
      ostrich.repr.clearAll()
      Stats.flush()
    }
  }
}
