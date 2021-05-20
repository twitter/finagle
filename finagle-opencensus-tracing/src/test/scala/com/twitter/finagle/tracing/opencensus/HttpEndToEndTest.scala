package com.twitter.finagle.tracing.opencensus

import com.twitter.finagle.context.Contexts
import com.twitter.finagle.http.{Request, Response, Status}
import com.twitter.finagle.tracing.opencensus.TracingOps._
import com.twitter.finagle.{Address, Http, Name, Service}
import com.twitter.util.{Await, Duration, Future}
import io.opencensus.contrib.http.util.HttpPropagationUtil
import io.opencensus.trace.{SpanContext, Tracing}
import io.opencensus.trace.propagation.TextFormat
import java.net.{InetAddress, InetSocketAddress}
import java.util.concurrent.atomic.AtomicReference
import org.scalatest.funsuite.AnyFunSuite

class HttpEndToEndTest extends AnyFunSuite {

  private def await[T](f: Future[T]): T =
    Await.result(f, Duration.fromSeconds(15))

  private def httpServer(
    textFormat: TextFormat = Tracing.getPropagationComponent.getB3Format
  ): Http.Server = {
    import com.twitter.finagle.tracing.opencensus.StackServerOps._
    Http.server.withOpenCensusTracing(textFormat)
  }

  private def httpClient(
    textFormat: TextFormat = Tracing.getPropagationComponent.getB3Format
  ): Http.Client = {
    import com.twitter.finagle.tracing.opencensus.StackClientOps._
    Http.client.withOpenCensusTracing(textFormat)
  }

  test("SpanContext is propagated when using CloudTraceFormat") {
    val spanCtx = new AtomicReference[SpanContext](SpanContext.INVALID)

    val svc: Service[Request, Response] = Service.mk { req =>
      val rep = Contexts.broadcast.get(TraceContextFilter.SpanContextKey) match {
        case None =>
          val rep = Response(Status.BadRequest)
          rep.contentString = "Broadcast context not set"
          rep
        case Some(ctx) if ctx.getTraceId != spanCtx.get.getTraceId =>
          val rep = Response(Status.BadRequest)
          rep.contentString =
            s"TraceId mismatch, expected ${spanCtx.get.getTraceId}, but was ${ctx.getTraceId}"
          rep
        case Some(_) =>
          Response(Status.Ok)
      }

      Future.value(rep)
    }
    val server = httpServer(HttpPropagationUtil.getCloudTraceFormat).serve(
      new InetSocketAddress(InetAddress.getLoopbackAddress, 0),
      svc
    )
    val client = httpClient(HttpPropagationUtil.getCloudTraceFormat).newService(
      Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])),
      "cheese-processor"
    )

    val span = Tracing.getTracer
      .spanBuilder("test")
      .startSpan()
    assert(span.getContext.isValid)
    span.scopedAndEnd {
      spanCtx.set(Tracing.getTracer.getCurrentSpan.getContext)

      val rep = await(client(Request()))
      assert(rep.status == Status.Ok, rep.contentString)
    }
    client.close()
    server.close()
  }

  test("SpanContext is propagated") {
    val spanCtx = new AtomicReference[SpanContext](SpanContext.INVALID)

    val svc: Service[Request, Response] = Service.mk { req =>
      val rep = Contexts.broadcast.get(TraceContextFilter.SpanContextKey) match {
        case None =>
          val rep = Response(Status.BadRequest)
          rep.contentString = "Broadcast context not set"
          rep
        case Some(ctx) if ctx.getTraceId != spanCtx.get.getTraceId =>
          val rep = Response(Status.BadRequest)
          rep.contentString =
            s"TraceId mismatch, expected ${spanCtx.get.getTraceId}, but was ${ctx.getTraceId}"
          rep
        case Some(_) =>
          Response(Status.Ok)
      }

      Future.value(rep)
    }
    val server = httpServer().serve(
      new InetSocketAddress(InetAddress.getLoopbackAddress, 0),
      svc
    )
    val client = httpClient().newService(
      Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])),
      "cheese-processor"
    )

    val span = Tracing.getTracer
      .spanBuilder("test")
      .startSpan()
    assert(span.getContext.isValid)
    span.scopedAndEnd {
      spanCtx.set(Tracing.getTracer.getCurrentSpan.getContext)

      val rep = await(client(Request()))
      assert(Status.Ok == rep.status, rep.contentString)
    }
    client.close()
    server.close()
  }

}
