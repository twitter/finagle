package com.twitter.finagle.tracing

import com.twitter.conversions.time._
import com.twitter.finagle.builder.{ClientBuilder, ServerBuilder}
import com.twitter.finagle.client.StringClient
import com.twitter.finagle.server.StringServer
import com.twitter.finagle.{param => fparam, _}
import com.twitter.util._
import java.net.{InetAddress, InetSocketAddress}
import org.scalatest.FunSuite
import org.scalatest.concurrent.{Eventually, IntegrationPatience}

class DefaultTracingTest extends FunSuite with Eventually with IntegrationPatience {
  object Svc extends Service[String, String] {
    def apply(str: String): Future[String] = Future.value(str)
  }

  def assertAnnotationsInOrder(tracer: Seq[Record], annos: Seq[Annotation]) {
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
  def testCoreTraces(f: (Tracer, Tracer) => (Service[String, String], Closable)) {
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
        Annotation.ClientSend(),
        Annotation.ClientRecv()
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
          Annotation.ServerRecv(),
          Annotation.ServerSend()
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

  test("core events are traced in the ClientBuilder/ServerBuilder") {
    testCoreTraces { (serverTracer, clientTracer) =>
      val svc = ServerBuilder()
        .name("theServer")
        .bindTo(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))
        .stack(StringServer.server)
        .tracer(serverTracer)
        .build(Svc)

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
}
