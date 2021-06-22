package com.twitter.finagle.tracing

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.client.utils.StringClient
import com.twitter.finagle.server.utils.StringServer
import com.twitter.finagle.{param => fparam, _}
import com.twitter.util._
import java.net.{InetAddress, InetSocketAddress}
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.funsuite.AnyFunSuite

class DefaultTracingTest extends AnyFunSuite with Eventually with IntegrationPatience {
  object Svc extends Service[String, String] {
    def apply(str: String): Future[String] = Future.value(str)
  }

  def assertAnnotationsInOrder(tracer: Seq[Record], annos: Seq[Annotation]): Unit = {
    assert(tracer.collect { case Record(_, _, ann, _) if annos.contains(ann) => ann } == annos)
  }

  /**
   * Ensure all annotations have the same TraceId (unique to server and client though)
   * Ensure core annotations are present and properly ordered
   *
   * @param f returns a [[Service]] and a [[Closable]] finalizer.
   *          The finalizer properly closes the services built by f,
   *          otherwise the tests here will prevent [[com.twitter.finagle.util.ExitGuard]] from exiting,
   *          and interfere with [[com.twitter.finagle.util.ExitGuardTest]]
   */
  def testCoreTraces(f: (Tracer, Tracer) => (Service[String, String], Closable)): Unit = {
    val serverTracer = new BufferingTracer
    val clientTracer = new BufferingTracer

    val (svc, finalizer) = f(serverTracer, clientTracer)
    Await.result(svc("foo"), 1.second)

    assert(serverTracer.map(_.traceId).toSet.size == 1)
    assert(clientTracer.map(_.traceId).toSet.size == 1)

    assertAnnotationsInOrder(
      clientTracer.toSeq,
      Seq(
        Annotation.ServiceName("theClient"),
        Annotation.ClientSend,
        Annotation.WireSend,
        Annotation.WireRecv,
        Annotation.ClientRecv
      )
    )

    // We need the `eventually` since the server may actually add the tracing annotation for
    // the response after sending the response based on it being done via a `respond` block
    // on the response `Future`, and we only await the clients response.
    eventually {
      assertAnnotationsInOrder(
        serverTracer.toSeq,
        Seq(
          Annotation.ServiceName("theServer"),
          Annotation.WireRecv,
          Annotation.ServerRecv,
          Annotation.ServerSend,
          Annotation.WireSend
        )
      )
    }

    // need to call finalizer to properly close the client and the server
    Await.ready(finalizer.close(), 1.second)
  }

  test("core events are traced in the stack client/server") {
    testCoreTraces { (serverTracer, clientTracer) =>
      val svc = StringServer.server
        .configured(fparam.Tracer(serverTracer))
        .configured(fparam.Label("theServer"))
        .serve("localhost:*", Svc)

      val client = StringClient.client
        .configured(fparam.Tracer(clientTracer))
        .newService(
          Name.bound(Address(svc.boundAddress.asInstanceOf[InetSocketAddress])),
          "theClient"
        )

      val finalizer = new Closable {
        override def close(deadline: Time): Future[Unit] =
          client.close(deadline) before svc.close(deadline)
      }

      (client, finalizer)
    }
  }

  test("core events are traced in the ClientBuilder") {
    testCoreTraces { (serverTracer, clientTracer) =>
      val svc = StringServer.server
        .withLabel("theServer")
        .withTracer(serverTracer)
        .serve(new InetSocketAddress(InetAddress.getLoopbackAddress, 0), Svc)

      val client = ClientBuilder()
        .name("theClient")
        .hosts(svc.boundAddress.asInstanceOf[InetSocketAddress])
        .stack(StringClient.client)
        .hostConnectionLimit(1)
        .tracer(clientTracer)
        .build()

      val finalizer = new Closable {
        override def close(deadline: Time): Future[Unit] =
          client.close(deadline) before svc.close(deadline)
      }

      (client, finalizer)
    }
  }

  test("TraceServiceName overrides local client/server names") {
    val serverTracer = new BufferingTracer
    val clientTracer = new BufferingTracer

    def assertServiceNames(server: String, client: String): Unit = {
      eventually(
        assert(serverTracer.toSeq.map(_.annotation).contains(Annotation.ServiceName(server)))
      )
      assert(clientTracer.toSeq.map(_.annotation).contains(Annotation.ServiceName(client)))
    }

    val server = StringServer.server
      .configured(fparam.Tracer(serverTracer))
      .configured(fparam.Label("theServer"))
      .serve("localhost:*", Svc)

    val client = StringClient.client
      .configured(fparam.Tracer(clientTracer))
      .newService(
        Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])),
        "theClient"
      )

    Await.result(client("foo"), 1.second)
    assertServiceNames("theServer", "theClient")

    val saved = TraceServiceName()
    try {
      TraceServiceName.set(Some("/p/fqn"))
      Await.result(client("foo"), 1.second)

      assertServiceNames("/p/fqn", "/p/fqn")
      assert(
        clientTracer.toSeq
          .map(_.annotation).contains(
            Annotation.BinaryAnnotation("clnt/finagle.label", "theClient")
          )
      )
      assert(
        serverTracer.toSeq
          .map(_.annotation).contains(Annotation.BinaryAnnotation("srv/finagle.label", "theServer"))
      )
    } finally {
      TraceServiceName.set(saved)
    }
  }
}
