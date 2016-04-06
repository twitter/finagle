package com.twitter.finagle.dispatch

import com.twitter.conversions.time._
import com.twitter.finagle._
import com.twitter.finagle.builder.{ClientBuilder, ServerBuilder}
import com.twitter.finagle.client._
import com.twitter.finagle.dispatch._
import com.twitter.finagle.netty3._
import com.twitter.finagle.param.ReqRepToTraceId
import com.twitter.finagle.server._
import com.twitter.finagle.Stack
import com.twitter.finagle.tracing.{Annotation, BufferingTracer, Record, Trace, TraceId, Tracer}
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

import com.twitter.finagle.tracing.TraceInitializerFilter

@RunWith(classOf[JUnitRunner])
final class ServerDispatcherTracingTest 
  extends FunSuite
  with StringClient
  with StringServer {

  private def testServerTraces(f: (Tracer) => (Service[String, String])) {
    val tracer = new BufferingTracer()

    Await.result(f(tracer)("foo"), 1.second)

    assert(tracer.map(_.traceId).toSet.size == 1)

    val validAnnos  = List(
      Annotation.WireRecv,
      Annotation.ServiceName("theServer"),
      Annotation.ServerRecv(),
      Annotation.ServerSend(),
      Annotation.WireSend)
    assertAnnotationsInOrder(tracer, validAnnos)
  }

  test("core events are traced in the stack server") {
    testServerTraces { (tracer) =>
      val svc = stringServer
        .withServerDispatcher.requestToTraceId(stringToTraceId)
        .withServerDispatcher.responseToTraceId(stringToTraceId)
        .configured(fparam.Tracer(tracer))
        .configured(fparam.Label("theServer"))
        .serve("localhost:*", Svc)

      stringClient
        .configured(fparam.Label("theClient"))
        .newService(svc)
    }
  }

  test("core events are traced in the DefaultServer") {
    testServerTraces { (serverTracer) =>
      val fs = new ReqRepToTraceId(stringToTraceId, stringToTraceId)

      val server = DefaultServer[String, String, String, String](
        name = "theServer",
        listener = Netty3Listener("theServer", StringServerPipeline),
        serviceTransport = serviceTransport,
        tracer = serverTracer,
        reqRepToTraceId = fs)

      val client = DefaultClient[String, String](
        name = "theClient",
        endpointer = Bridge[String, String, String, String](
          Netty3Transporter("theClient", StringClientPipeline), new SerialClientDispatcher(_))
        )

      val svc = server.serve("localhost:*", Svc)
      client.newService(svc)
    }
  }

  test("core events are traced in the ServerBuilder") {
    testServerTraces { (serverTracer) =>
      val svc = ServerBuilder()
        .name("theServer")
        .bindTo(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))
        .codec(StringServerCodec)
        .tracer(serverTracer)
        .requestToTraceId(stringToTraceId)
        .responseToTraceId(stringToTraceId)
        .build(Svc)

      ClientBuilder()
        .name("theClient")
        .hosts(svc.boundAddress.asInstanceOf[InetSocketAddress])
        .codec(StringClientCodec)
        .hostConnectionLimit(1)
        .build()
    }
  }

  private def assertAnnotationsInOrder(tracer: BufferingTracer, 
    validAnnos: Seq[Annotation]): Unit = {
    val annos = tracer.toSeq
      .map(_.annotation)
      .filter(validAnnos.contains(_))
    assert(annos.sameElements(validAnnos) === true,
      failureMessageForTracerComparison(annos, validAnnos))
  }

  private def failureMessageForTracerComparison(annos: Seq[Annotation], 
    validAnnos: Seq[Annotation]): String = {
    val annosStr = annos.foldLeft("")(_ + " " + _.toString)
    val validAnnosStr = validAnnos.foldLeft("")(_ + " " + _.toString)
    s"Actual Annotations $annos did not match Expected Annotations $validAnnos"
  }

  private object Svc extends Service[String, String] {
    def apply(str: String): Future[String] = Future.value(str)
  }
  
  private val serviceTransport: (Transport[String, String], Service[String, String],
    ServerDispatcherInitializer) => Closable = 
        (t: Transport[String, String], s: Service[String, String], 
          sdi: ServerDispatcherInitializer) => 
          new SerialServerDispatcher(t, s, sdi)
  
  private val stringToTraceId = (s: Any) => s match {
    case s: String => Some(Trace.id)
    case _         => None
  }
}

private object StringServerCodec extends com.twitter.finagle.Codec[String, String] {
  val pipelineFactory = StringServerPipeline

  override def newTraceInitializer = TraceInitializerFilter.empty[String, String]
}

private object StringClientCodec extends com.twitter.finagle.Codec[String, String] {
  val pipelineFactory = StringClientPipeline
}
