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
    state.stats.scope(state.next())

  @annotations.Benchmark
  def counter(): Counter =
    state.stats.counter(state.next())

  @annotations.Benchmark
  def stat(): Stat =
    state.stats.stat(state.next())

  @annotations.Benchmark
  def scopeAndScope(): StatsReceiver =
    state.stats.scope(state.next()).scope(state.next())

  @annotations.Benchmark
  def scopeAndCounter(): Counter =
    state.stats.scope(state.next()).counter(state.next())

  @annotations.Benchmark
  def scopeAndStat(): Stat =
    state.stats.scope(state.next()).stat(state.next())

  @annotations.Benchmark
  def scopedCounter(): Counter =
    state.stats.counter(state.next(), state.next())

  @annotations.Benchmark
  def scopedStat(): Stat =
    state.stats.stat(state.next(), state.next())
}

object StatsReceiverBenchmark {

  val Names = Array.fill(1000)(Random.alphanumeric.take(12).mkString)

  class State(val stats: StatsReceiver) {
    private var i = 0
    def next(): String =
      try Names(i)
      finally i = if (i + 1 == Names.length) 0 else i + 1
  }

  final class MetricsState extends State(new MetricsStatsReceiver(Metrics.createDetached()))

  final class MemoizingState
      extends State(new Memoizing(new MetricsStatsReceiver(Metrics.createDetached())))

  final class Memoizing(val self: StatsReceiver)
      extends StatsReceiver
      with DelegatingStatsReceiver
      with Proxy {

    def underlying: Seq[StatsReceiver] = Seq(self)

    val repr = self.repr

    private[this] lazy val scopeMemo =
      Memoize[String, StatsReceiver] { name =>
        new Memoizing(self.scope(name))
      }

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

    override def counter(verbosity: Verbosity, name: String*): Counter =
      counterMemo(name -> verbosity)

    override def stat(verbosity: Verbosity, name: String*): Stat = statMemo(name -> verbosity)

    override def addGauge(verbosity: Verbosity, name: String*)(f: => Float): Gauge = {
      // scalafix:off StoreGaugesAsMemberVariables
      self.addGauge(verbosity, name: _*)(f)
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
