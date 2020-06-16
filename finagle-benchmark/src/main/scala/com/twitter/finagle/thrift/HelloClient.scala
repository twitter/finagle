package com.twitter.finagle.thrift

import com.twitter.finagle.ThriftMux
import com.twitter.finagle.benchmark.StdBenchAnnotations
import com.twitter.finagle.benchmark.thriftscala._
import com.twitter.util.{Await, Future}
import com.twitter.finagle.param
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.tracing.NullTracer
import org.openjdk.jmh.annotations.{Benchmark, Scope, State}

/**
 * Thrift client allocations benchmark.
 *
 * Usage
 *
 * 1. Run [[HelloServer]] as a separate process to isolate client performance:
 * $ pants run finagle/finagle-benchmark/src/main/scala:hello-server
 *
 * 2. Run the client benchmark in sbt:
 * $ ./sbt
 * > project finagle-benchmark
 * > run HelloClient -prof gc [-wi 20 -f 4]
 */
@State(Scope.Benchmark)
class HelloClient extends StdBenchAnnotations {
  val svc: Hello.MethodPerEndpoint = ThriftMux.client
    .configured(param.Tracer(NullTracer))
    .configured(param.Stats(NullStatsReceiver))
    .build[Hello.MethodPerEndpoint]("localhost:1234")

  @Benchmark
  def helloClient(): String = {
    Await.result(svc.echo("asdf"))
  }
}
