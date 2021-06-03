package com.twitter.finagle.stats

import com.twitter.finagle.benchmark.StdBenchAnnotations
import com.twitter.util.Memoize
import org.openjdk.jmh.annotations
import scala.util.Random

@annotations.State(annotations.Scope.Benchmark)
@annotations.Warmup(iterations = 5, time = 2)
@annotations.Measurement(iterations = 5, time = 2)
abstract class StatsReceiverBenchmark extends StdBenchAnnotations {

  protected val state: StatsReceiverBenchmark.State

  @annotations.Benchmark
  def scope(): StatsReceiver =
    state.stats.scope(state.nextString())

  @annotations.Benchmark
  def counter(): Counter =
    state.stats.counter(state.nextString())

  @annotations.Benchmark
  def stat(): Stat =
    state.stats.stat(state.nextString())

  @annotations.Benchmark
  def incr(): Unit = state.counter.incr()

  @annotations.Benchmark
  def add(): Unit = state.stat.add(state.nextNatural())

  @annotations.Benchmark
  def counterAndIncr(): Counter = {
    val ctr = state.stats.counter(state.nextString())
    ctr.incr()
    ctr
  }

  @annotations.Benchmark
  def statAndAdd(): Stat = {
    val st = state.stats.stat(state.nextString())
    st.add(state.nextNatural())
    st
  }

  @annotations.Benchmark
  def scopeAndScope(): StatsReceiver =
    state.stats.scope(state.nextString()).scope(state.nextString())

  @annotations.Benchmark
  def scopeAndCounter(): Counter =
    state.stats.scope(state.nextString()).counter(state.nextString())

  @annotations.Benchmark
  def scopeAndStat(): Stat =
    state.stats.scope(state.nextString()).stat(state.nextString())

  @annotations.Benchmark
  def scopedCounter(): Counter =
    state.stats.counter(state.nextString(), state.nextString())

  @annotations.Benchmark
  def scopedStat(): Stat =
    state.stats.stat(state.nextString(), state.nextString())
}

object StatsReceiverBenchmark {

  val Names = Array.fill(1000)(Random.alphanumeric.take(12).mkString)
  val Naturals = Array.fill(1000)(Random.nextInt().toFloat)

  abstract class State(val stats: StatsReceiver) {
    private var i = 0
    def nextString(): String =
      try Names(i)
      finally i = if (i + 1 == Names.length) 0 else i + 1

    private var j = 0
    def nextNatural(): Float = {
      try Naturals(j)
      finally j = if (j + 1 == Naturals.length) 0 else j + 1
    }

    val counter: Counter = stats.counter("foo")

    val stat: Stat = stats.stat("bar")
  }

  final class MetricsState extends State(new MetricsStatsReceiver(Metrics.createDetached()))

  final class MemoizingState
      extends State(new Memoizing(new MetricsStatsReceiver(Metrics.createDetached())))

  final class LazyState
      extends State(new LazyStatsReceiver(new MetricsStatsReceiver(Metrics.createDetached())))

  final class Memoizing(val self: StatsReceiver)
      extends StatsReceiver
      with DelegatingStatsReceiver
      with Proxy {

    def underlying: Seq[StatsReceiver] = Seq(self)

    val repr = self.repr

    private[this] lazy val scopeMemo =
      Memoize[String, StatsReceiver] { name => new Memoizing(self.scope(name)) }

    private[this] lazy val counterMemo =
      Memoize[(Seq[String], Verbosity), Counter] {
        case (names, verbosity) =>
          self.counter(verbosity, names: _*)
      }

    private[this] lazy val statMemo =
      Memoize[(Seq[String], Verbosity), Stat] {
        case (names, verbosity) =>
          self.stat(verbosity, names: _*)
      }

    def counter(metricBuilder: MetricBuilder): Counter =
      counterMemo(metricBuilder.name -> metricBuilder.verbosity)

    def stat(metricBuilder: MetricBuilder): Stat =
      statMemo(metricBuilder.name -> metricBuilder.verbosity)

    def addGauge(metricBuilder: MetricBuilder)(f: => Float): Gauge = {
      // scalafix:off StoreGaugesAsMemberVariablesStatsReceiverProxyStatsReceiverProxy
      self.addGauge(metricBuilder)(f)
      // scalafix:on StoreGaugesAsMemberVariables
    }

    override def scope(name: String): StatsReceiver = scopeMemo(name)
  }
}

class MetricsStatsReceiverBenchmark extends StatsReceiverBenchmark {
  protected val state = new StatsReceiverBenchmark.MetricsState
}

class MemoizingStatsReceiverBenchmark extends StatsReceiverBenchmark {
  protected val state = new StatsReceiverBenchmark.MemoizingState
}

class LazyStatsReceiverBenchmark extends StatsReceiverBenchmark {
  protected val state = new StatsReceiverBenchmark.LazyState
}
