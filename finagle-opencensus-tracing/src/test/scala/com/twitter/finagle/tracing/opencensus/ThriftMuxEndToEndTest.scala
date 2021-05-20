package com.twitter.finagle.tracing.opencensus

import com.twitter.finagle.context.Contexts
import com.twitter.finagle.thriftmux.thriftscala.TestService
import com.twitter.finagle.{Address, Name, ThriftMux}
import com.twitter.finagle.tracing.opencensus.TracingOps._
import com.twitter.util.{Await, Duration, Future}
import io.opencensus.trace.{SpanContext, Tracing}
import java.net.{InetAddress, InetSocketAddress}
import java.util.concurrent.atomic.AtomicReference
import org.scalatest.funsuite.AnyFunSuite

class ThriftMuxEndToEndTest extends AnyFunSuite {

  private def await[T](f: Future[T]): T =
    Await.result(f, Duration.fromSeconds(15))

  private def thriftServer: ThriftMux.Server = {
    import com.twitter.finagle.tracing.opencensus.StackServerOps._
    ThriftMux.server.withOpenCensusTracing
  }

  private def thriftClient: ThriftMux.Client = {
    import com.twitter.finagle.tracing.opencensus.StackClientOps._
    ThriftMux.client.withOpenCensusTracing
  }

  test("SpanContext is propagated") {
    val spanCtx = new AtomicReference[SpanContext](SpanContext.INVALID)

    val success = "TraceIds matched"

    val thriftService = new TestService.MethodPerEndpoint {
      def query(x: String): Future[String] = {
        val rep = Contexts.broadcast.get(TraceContextFilter.SpanContextKey) match {
          case None =>
            "Broadcast context not set"
          case Some(ctx) if ctx.getTraceId != spanCtx.get.getTraceId =>
            s"TraceId mismatch, expected ${spanCtx.get.getTraceId}, but was ${ctx.getTraceId}"
          case Some(ctx) =>
            success
        }
        Future.value(rep)
      }
      def inquiry(z: String): Future[String] = ???
      def question(y: String): Future[String] = ???
    }

    val server = thriftServer
      .serveIface(
        new InetSocketAddress(InetAddress.getLoopbackAddress, 0),
        thriftService
      )

    val client = thriftClient
      .build[TestService.MethodPerEndpoint](
        dest = Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])),
        label = "thriftmux_server"
      )

    val span = Tracing.getTracer
      .spanBuilder("test")
      .startSpan()
    assert(span.getContext.isValid)
    span.scopedAndEnd {
      spanCtx.set(Tracing.getTracer.getCurrentSpan.getContext)

      val rep = await(client.query("cheese"))
      assert(success == rep)
    }
    client.asClosable.close()
    server.close()
  }

}
