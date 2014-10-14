package com.twitter.finagle.http

import com.twitter.finagle.{param, Http => FHttp, Service}
import com.twitter.finagle.builder.{ClientBuilder, ServerBuilder}
import com.twitter.finagle.tracing._
import com.twitter.util.{Await, Future, RandomSocket}
import java.net.InetSocketAddress
import org.jboss.netty.handler.codec.http.{HttpRequest, HttpResponse}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

private object Svc extends Service[HttpRequest, HttpResponse] {
  def apply(req: HttpRequest): Future[HttpResponse] = {
    Future.value(Request(req).response)
  }
}

@RunWith(classOf[JUnitRunner])
class TraceInitializationTest extends FunSuite {
  val req = RequestBuilder().url("http://foo/this/is/a/uri/path").buildGet()

  def assertAnnotationsInOrder(records: Seq[Record], annos: Seq[Annotation]) {
    assert(records.collect { case Record(_, _, ann, _) if annos.contains(ann) => ann } === annos)
  }

  /**
   * Ensure all annotations have the same TraceId (it should be passed between client and server)
   * Ensure core annotations are present and properly ordered
   */
  def testTraces(f: (Tracer, Tracer) => (Service[HttpRequest, HttpResponse])) {
    val tracer = new BufferingTracer

    Await.result(f(tracer, tracer)(req))

    assertAnnotationsInOrder(tracer.toSeq, Seq(
      Annotation.Rpc("GET"),
      Annotation.BinaryAnnotation("http.uri", "/this/is/a/uri/path"),
      Annotation.ServiceName("theClient"),
      Annotation.ClientSend(),
      Annotation.Rpc("GET"),
      Annotation.BinaryAnnotation("http.uri", "/this/is/a/uri/path"),
      Annotation.ServiceName("theServer"),
      Annotation.ServerRecv(),
      Annotation.ServerSend(),
      Annotation.ClientRecv()))

    assert(tracer.map(_.traceId).toSet.size === 1)

  }

  test("TraceId is propagated through the protocol") {
    testTraces { (serverTracer, clientTracer) =>
      val port = RandomSocket.nextPort()
      FHttp.server.configured(param.Tracer(serverTracer)).serve("theServer=:" + port, Svc)
      FHttp.client.configured(param.Tracer(clientTracer)).newService("theClient=:" + port)
    }
  }

  test("TraceId is propagated through the protocol (builder)") {
    testTraces { (serverTracer, clientTracer) =>
      val socket = new java.net.InetSocketAddress(RandomSocket.nextPort())
      ServerBuilder()
        .name("theServer")
        .bindTo(socket)
        .codec(Http(_enableTracing = true))
        .tracer(serverTracer)
        .build(Svc)

      ClientBuilder()
        .name("theClient")
        .hosts(socket)
        .codec(Http(_enableTracing = true))
        .hostConnectionLimit(1)
        .tracer(clientTracer)
        .build()
    }
  }
}
