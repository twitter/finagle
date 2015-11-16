package com.twitter.finagle.http

import com.twitter.finagle.builder.{ClientBuilder, ServerBuilder}
import com.twitter.finagle.tracing._
import com.twitter.finagle.{Service, param}
import com.twitter.util.{Await, Closable, Future}
import java.net.{InetAddress, InetSocketAddress}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

private object Svc extends Service[Request, Response] {
  def apply(req: Request) = Future.value(req.response)
}

@RunWith(classOf[JUnitRunner])
class TraceInitializationTest extends FunSuite {
  def req = RequestBuilder().url("http://foo/this/is/a/uri/path").buildGet()

  def assertAnnotationsInOrder(records: Seq[Record], annos: Seq[Annotation]) {
    assert(records.collect { case Record(_, _, ann, _) if annos.contains(ann) => ann } == annos)
  }

  /**
   * Ensure all annotations have the same TraceId (it should be passed between client and server)
   * Ensure core annotations are present and properly ordered
   */
  def testTraces(f: (Tracer, Tracer) => (Service[Request, Response], Closable)) {
    val tracer = new BufferingTracer

    val (svc, closable) = f(tracer, tracer)
    try Await.result(svc(req)) finally {
      Closable.all(svc, closable).close()
    }

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

    assert(tracer.map(_.traceId).toSet.size == 1)
  }

  test("TraceId is propagated through the protocol") {
    testTraces { (serverTracer, clientTracer) =>
      import com.twitter.finagle
      val server = finagle.Http.server
        .configured(param.Tracer(serverTracer))
        .configured(param.Label("theServer")).serve(":*", Svc)
      val port = server.boundAddress.asInstanceOf[InetSocketAddress].getPort
      val client = finagle.Http.client
        .configured(param.Tracer(clientTracer)).newService(":" + port, "theClient")
      (client, server)
    }
  }

  test("TraceId is propagated through the protocol (builder)") {
    testTraces { (serverTracer, clientTracer) =>
      val server = ServerBuilder()
        .name("theServer")
        .bindTo(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))
        .codec(Http(_enableTracing = true))
        .tracer(serverTracer)
        .build(Svc)

      val port = server.boundAddress.asInstanceOf[InetSocketAddress].getPort
      val client = ClientBuilder()
        .name("theClient")
        .hosts(s"localhost:$port")
        .codec(Http(_enableTracing = true))
        .hostConnectionLimit(1)
        .tracer(clientTracer)
        .build()
      (client, server)
    }
  }

  test("TraceId is set when a client does not propagate one") {
    import com.twitter.finagle
    val tracer = new BufferingTracer

    val server = finagle.Http.server
      .configured(param.Tracer(tracer))
      .configured(param.Label("theServer")).serve(":*", Svc)
    try {
      val port = server.boundAddress.asInstanceOf[InetSocketAddress].getPort
      val client = ClientBuilder()
        .name("theClient")
        .hosts(s"localhost:$port")
        .codec(Http(_enableTracing = false))
        .hostConnectionLimit(1)
        .build()
      try {
        0.until(2).foreach { _ =>
          Await.result(client(req))
        }

        assert(tracer.map(_.traceId).toSet.size == 2)
      } finally client.close()
    } finally server.close()
  }
}
