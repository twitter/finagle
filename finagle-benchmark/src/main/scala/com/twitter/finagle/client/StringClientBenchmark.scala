package com.twitter.finagle.client

import com.twitter.finagle.Name
import com.twitter.finagle.Service
import com.twitter.finagle.benchmark.StdBenchAnnotations
import com.twitter.finagle.client.utils.StringClient
import com.twitter.finagle.tracing.Flags
import com.twitter.finagle.tracing.Trace
import com.twitter.util.Await
import com.twitter.util.Future
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.State

@State(Scope.Benchmark)
class StringClientBenchmark extends StdBenchAnnotations {

  // service from a full finagle client
  val service = StringClient.client
    .newService(Name.bound(Service.mk[String, String](str => Future.value(str))), "serviceClient")
  val request = "foo"

  val nonSampledTraceId = Trace.nextId.copy(flags = Flags().setFlag(Flags.SamplingKnown))
  val sampledTraceId = Trace.nextId.copy(flags = Flags().setDebug)

  @Benchmark
  def applyNonTraced(): String = {
    Await.result(Trace.letId(nonSampledTraceId)(service(request)))
  }

  @Benchmark
  def applyTraced(): String = {
    Await.result(Trace.letId(sampledTraceId)(service(request)))
  }

  @Benchmark
  def applyUnsampled(): String = {
    Await.result(Trace.letClear(service(request)))
  }
}
