package com.twitter.finagle.service

import com.twitter.conversions.time._
import com.twitter.util.Duration
import com.twitter.finagle.benchmark.StdBenchAnnotations
import org.openjdk.jmh.annotations.{Scope, State, Benchmark}

// ./sbt 'project finagle-benchmark' 'run .*BackoffBenchmark.*'
class BackoffBenchmark extends StdBenchAnnotations {
  import BackoffBenchmark._

  @Benchmark
  def fromFunction(state: FromFunction): Duration = state.next()

  @Benchmark
  def constant(state: Constant): Duration = state.next()

  @Benchmark
  def equalJittered(state: EqualJittered): Duration = state.next()

  @Benchmark
  def exponentialJittered(state: ExponentialJittered): Duration = state.next()

  @Benchmark
  def decorrelatedJittered(state: DecorrelatedJittered): Duration = state.next()
}

object BackoffBenchmark {

  abstract class BackoffState(var backoff: Stream[Duration]) {
    def next(): Duration = {
      val head = backoff.head
      backoff = backoff.tail
      head
    }
  }

  @State(Scope.Thread)
  class FromFunction extends BackoffState(
    Backoff.fromFunction(() => 10.seconds) ++ Backoff.const(300.seconds)
  )

  @State(Scope.Thread)
  class Constant extends BackoffState(
    Backoff.const(10.seconds) ++ Backoff.const(300.seconds)
  )

  @State(Scope.Thread)
  class EqualJittered extends BackoffState(
    Backoff.equalJittered(5.seconds, 300.seconds) ++ Backoff.const(300.seconds)
  )

  @State(Scope.Thread)
  class ExponentialJittered extends BackoffState(
    Backoff.exponentialJittered(5.second, 300.seconds) ++ Backoff.const(300.seconds)
  )

  @State(Scope.Thread)
  class DecorrelatedJittered extends BackoffState(
    Backoff.decorrelatedJittered(5.second, 300.seconds) ++ Backoff.const(300.seconds)
  )
}
