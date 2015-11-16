package com.twitter.finagle.tracing

import com.twitter.conversions.time._
import com.twitter.finagle.Stack
import com.twitter.finagle._
import com.twitter.finagle.builder.{ClientBuilder, ServerBuilder}
import com.twitter.finagle.client._
import com.twitter.finagle.dispatch._
import com.twitter.finagle.netty3._
import com.twitter.finagle.server._
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.{param => fparam}
import com.twitter.io.Charsets
import com.twitter.util._
import java.net.{InetAddress, SocketAddress, InetSocketAddress}
import org.jboss.netty.channel.{Channels, ChannelPipelineFactory}
import org.jboss.netty.handler.codec.string.{StringEncoder, StringDecoder}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

private object StringServerCodec extends com.twitter.finagle.Codec[String, String] {
  val pipelineFactory = StringServerPipeline
}

private object StringClientCodec extends com.twitter.finagle.Codec[String, String] {
  val pipelineFactory = StringClientPipeline
}

@RunWith(classOf[JUnitRunner])
class DefaultTracingTest extends FunSuite with StringClient with StringServer {
  object Svc extends Service[String, String] {
    def apply(str: String): Future[String] = Future.value(str)
  }

  def assertAnnotationsInOrder(tracer: Seq[Record], annos: Seq[Annotation]) {
    assert(tracer.collect { case Record(_, _, ann, _) if annos.contains(ann) => ann } == annos)
  }

  /**
   * Ensure all annotations have the same TraceId (unique to server and client though)
   * Ensure core annotations are present and properly ordered
   */
  def testCoreTraces(f: (Tracer, Tracer) => (Service[String, String])) {
    val combinedTracer = new BufferingTracer
    class MultiTracer extends BufferingTracer {
      override def record(rec: Record) {
        super.record(rec)
        combinedTracer.record(rec)
      }
    }
    val serverTracer = new MultiTracer
    val clientTracer = new MultiTracer

    Await.result(f(serverTracer, clientTracer)("foo"), 1.second)

    assert(serverTracer.map(_.traceId).toSet.size == 1)
    assert(clientTracer.map(_.traceId).toSet.size == 1)

    assertAnnotationsInOrder(combinedTracer.toSeq, Seq(
      Annotation.ServiceName("theClient"),
      Annotation.ClientSend(),
      Annotation.ServiceName("theServer"),
      Annotation.ServerRecv(),
      Annotation.ServerSend(),
      Annotation.ClientRecv()))
  }

  test("core events are traced in the stack client/server") {
    testCoreTraces { (serverTracer, clientTracer) =>
      val svc = stringServer
        .configured(fparam.Tracer(serverTracer))
        .configured(fparam.Label("theServer"))
        .serve("localhost:*", Svc)

      stringClient
        .configured(fparam.Tracer(clientTracer))
        .configured(fparam.Label("theClient"))
        .newService(svc)
    }
  }

  test("core events are traced in the DefaultClient/DefaultServer") {
    testCoreTraces { (serverTracer, clientTracer) =>
      val server = DefaultServer[String, String, String, String](
        name = "theServer",
        listener = Netty3Listener("theServer", StringServerPipeline),
        serviceTransport = new SerialServerDispatcher(_, _),
        tracer = serverTracer)

      val client = DefaultClient[String, String](
        name = "theClient",
        endpointer = Bridge[String, String, String, String](
          Netty3Transporter("theClient", StringClientPipeline), new SerialClientDispatcher(_)),
        tracer = clientTracer)

      val svc = server.serve("localhost:*", Svc)
      client.newService(svc)
    }
  }

  test("core events are traced in the ClientBuilder/ServerBuilder") {
    testCoreTraces { (serverTracer, clientTracer) =>
      val svc = ServerBuilder()
        .name("theServer")
        .bindTo(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))
        .codec(StringServerCodec)
        .tracer(serverTracer)
        .build(Svc)

      ClientBuilder()
        .name("theClient")
        .hosts(svc.boundAddress)
        .codec(StringClientCodec)
        .hostConnectionLimit(1)
        .tracer(clientTracer)
        .build()
    }
  }
}
