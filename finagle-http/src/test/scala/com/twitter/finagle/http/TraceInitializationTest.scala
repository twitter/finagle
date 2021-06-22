package com.twitter.finagle.http

import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.tracing._
import com.twitter.finagle.Service
import com.twitter.util.{Await, Closable, Future}
import java.net.{InetAddress, InetSocketAddress}
import org.scalatest.funsuite.AnyFunSuite

private object Svc extends Service[Request, Response] {
  def apply(req: Request) = Future.value(req.response)
}

class TraceInitializationTest extends AnyFunSuite {
  def req = RequestBuilder().url("http://foo/this/is/a/uri/path").buildGet()

  def assertAnnotationsInOrder(records: Seq[Record], annos: Seq[Annotation]): Unit = {
    assert(records.collect { case Record(_, _, ann, _) if annos.contains(ann) => ann } == annos)
  }

  /**
   * Ensure all annotations have the same TraceId (it should be passed between client and server)
   * Ensure core annotations are present and properly ordered
   */
  def testTraces(f: (Tracer, Tracer) => (Service[Request, Response], Closable)): Unit = {
    val tracer = new BufferingTracer

    val (svc, closable) = f(tracer, tracer)
    try Await.result(svc(req))
    finally {
      Closable.all(svc, closable).close()
    }

    assertAnnotationsInOrder(
      tracer.toSeq,
      Seq(
        Annotation.ServiceName("theClient"),
        Annotation.ClientSend,
        Annotation.Rpc("GET"),
        Annotation.BinaryAnnotation("http.method", "GET"),
        Annotation.BinaryAnnotation("http.uri", "/this/is/a/uri/path"),
        Annotation.ServiceName("theServer"),
        Annotation.ServerRecv,
        Annotation.Rpc("GET"),
        Annotation.BinaryAnnotation("http.method", "GET"),
        Annotation.BinaryAnnotation("http.uri", "/this/is/a/uri/path"),
        Annotation.ServerSend,
        Annotation.BinaryAnnotation("http.status_code", 200),
        Annotation.BinaryAnnotation("http.status_code", 200),
        Annotation.ClientRecv
      )
    )

    assert(tracer.map(_.traceId).toSet.size == 1)
  }

  test("TraceId is propagated through the protocol") {
    testTraces { (serverTracer, clientTracer) =>
      import com.twitter.finagle
      val server = finagle.Http.server
        .withTracer(serverTracer)
        .withLabel("theServer")
        .serve(":*", Svc)
      val port = server.boundAddress.asInstanceOf[InetSocketAddress].getPort
      val client = finagle.Http.client
        .withTracer(clientTracer)
        .newService(":" + port, "theClient")
      (client, server)
    }
  }

  test("TraceId is propagated through the protocol (builder)") {
    import com.twitter.finagle
    testTraces { (serverTracer, clientTracer) =>
      val server = finagle.Http.server
        .withLabel("theServer")
        .withTracer(serverTracer)
        .serve(new InetSocketAddress(InetAddress.getLoopbackAddress, 0), Svc)

      val port = server.boundAddress.asInstanceOf[InetSocketAddress].getPort
      val client = ClientBuilder()
        .name("theClient")
        .hosts(s"localhost:$port")
        .stack(finagle.Http.client)
        .hostConnectionLimit(1)
        .tracer(clientTracer)
        .build()
      (client, server)
    }
  }
}
