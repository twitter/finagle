package com.twitter.finagle.server

import com.twitter.finagle.Service
import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.Stack
import com.twitter.finagle.benchmark.StdBenchAnnotations
import com.twitter.finagle.server.utils.StringServer
import com.twitter.finagle.tracing.Flags
import com.twitter.finagle.tracing.ServerTracingFilter
import com.twitter.finagle.tracing.Trace
import com.twitter.util.Await
import com.twitter.util.Future
import org.openjdk.jmh.annotations._

@State(Scope.Benchmark)
class StringServerBenchmark extends StdBenchAnnotations {
  val echoStringModule = new Stack.Module0[ServiceFactory[String, String]] {
    val description = "echo module"
    val role = Stack.Role("echo")
    val echoSvc = Service.mk[String, String](str => Future.value(str))
    def make(next: ServiceFactory[String, String]): ServiceFactory[String, String] = {
      ServiceFactory.const(echoSvc)
    }
  }

  val echoServerService: Service[String, String] = {
    val srv = StringServer.server

    // we insert the stack after serverTracingFilter so that,
    // we can catch the metrics for the Annotated request with Server specific records
    // meaning finagle information receive/send events
    val stk = srv.stack.insertAfter(ServerTracingFilter.role, echoStringModule)
    val svcFac = stk.make(srv.params)
    Await.result(svcFac())
  }

  val request = "foo"
  val nonSampledTraceId = Trace.nextId.copy(flags = Flags().setFlag(Flags.SamplingKnown))
  val sampledTraceId = Trace.nextId.copy(flags = Flags().setDebug)

  @Benchmark
  def applyNonTraced(): String = {
    Await.result(Trace.letId(nonSampledTraceId)(echoServerService(request)))
  }

  @Benchmark
  def applyTraced(): String = {
    Await.result(Trace.letId(sampledTraceId)(echoServerService(request)))
  }

  @Benchmark
  def applyUnsampled(): String = {
    Await.result(Trace.letClear(echoServerService(request)))
  }
}
