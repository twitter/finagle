package com.twitter.finagle.netty4

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Stack.Params
import com.twitter.finagle.netty4.Netty4Listener.{BackPressure, MaxConnections}
import com.twitter.finagle.netty4.Netty4ListenerHelpers._
import com.twitter.finagle.param.{Label, Stats}
import com.twitter.finagle.{ListeningServer, Service}
import com.twitter.util._
import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel.ChannelPipeline
import java.io.IOException
import java.net.{InetAddress, InetSocketAddress, Socket, SocketAddress}
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import scala.util.control.NonFatal
import org.scalatest.funsuite.AnyFunSuite

abstract class AbstractNetty4ListenerTest
    extends AnyFunSuite
    with Eventually
    with IntegrationPatience {

  private val noopService: Service[Any, Nothing] = Service.const(Future.never)

  protected def serveService[Req: Manifest, Rep: Manifest](
    address: SocketAddress,
    init: ChannelPipeline => Unit,
    params: Params,
    service: Service[Req, Rep]
  ): ListeningServer

  protected final def serveService[Req: Manifest, Rep: Manifest](
    init: ChannelPipeline => Unit,
    params: Params,
    service: Service[Req, Rep]
  ): ListeningServer =
    serveService(new InetSocketAddress(InetAddress.getLoopbackAddress, 0), init, params, service)

  test("frames pipeline messages and forms a complete pipeline (aka it works end-to-end)") {
    val ctx = new StatsCtx
    import ctx._

    object TestService extends Service[String, String] {
      @volatile var observedRequest: Option[String] = None

      def apply(request: String) = {
        observedRequest = Some(request)
        Future.value("hi2u")
      }
    }

    val p = Params.empty + Label("test") + Stats(sr)
    val server = serveService(StringServerInit, p, TestService)

    val client = new Socket()
    eventually { client.connect(server.boundAddress) }
    client.getOutputStream.write("hello netty4!\n".getBytes("UTF-8"))
    client.getOutputStream.flush()

    eventually { assert(TestService.observedRequest == Some("hello netty4!")) }

    val response = client.getInputStream
    val expected = "hi2u"
    val actual = new String(Array.fill("hi2u".length)(response.read().toByte))
    assert(actual == expected)
    Await.ready(server.close(), 2.seconds)
  }

  test("bytebufs in default pipeline have refCnt == 1") {
    val ctx = new StatsCtx
    import ctx._

    object TestService extends Service[ByteBuf, ByteBuf] {
      val requestBB = new Promise[ByteBuf]

      def apply(request: ByteBuf) = {
        requestBB.setValue(request)
        Future.value(Unpooled.wrappedBuffer("hi".getBytes("UTF-8")))
      }
    }

    val p = Params.empty + Label("test") + Stats(sr)
    val server = serveService(_ => (), p, TestService)

    val client = new Socket()
    eventually { client.connect(server.boundAddress) }
    client.getOutputStream.write("hello netty4!\n".getBytes("UTF-8"))
    client.getOutputStream.flush()

    val bb = Await.result(TestService.requestBB, 2.seconds)
    assert(bb.refCnt == 1)
    Await.ready(server.close(), 2.seconds)
  }

  test("We can limit the max number of sockets") {
    val ctx = new StatsCtx
    import ctx._
    val p = Params.empty + Label("test") + Stats(sr) + MaxConnections(1)
    val server = serveService(_ => (), p, noopService)

    // Establish a connection and wait until the server sees it before we
    // start the second connection since through connection establishment
    // they could race with one another to be 'first'.
    val client1 = new Socket()
    client1.connect(server.boundAddress)
    eventually { assert(sr.gauges(Seq("connections"))() == 1.0f) }

    val client2 = new Socket()
    client2.connect(server.boundAddress)

    eventually {
      intercept[IOException] {
        client2.getOutputStream.write(1)
        client2.getOutputStream.flush()
      }
    }

    // Make sure that client1 is still able to write some stuff.
    client1.getOutputStream.write(1)
    client1.getOutputStream.flush()

    client1.close()
    client2.close()
    Await.ready(server.close(), 2.seconds)
  }

  test("bind failures are rethrown synchronously") {
    val ss = new java.net.ServerSocket(0, 10, InetAddress.getLoopbackAddress)
    val noopService = Service.const(Future.exception(new Exception("sadface")))
    intercept[java.net.BindException] {
      serveService(ss.getLocalSocketAddress, _ => (), Params.empty, noopService)
    }
    ss.close()
  }

  test("records basic channel stats") {
    val ctx = new StatsCtx
    import ctx._

    // need to turn off backpressure since we don't read off the transport
    val p = Params.empty + Label("srv") + Stats(sr) + BackPressure(false)

    val server1 = serveService(_ => (), p, noopService)
    val server2 = serveService(_ => (), p, noopService)

    val (client1, client2) = (new Socket(), new Socket())

    eventually { client1.connect(server1.boundAddress) }
    eventually { client2.connect(server2.boundAddress) }

    eventually { counterEquals("connects")(2) }

    client1.getOutputStream.write(Array[Byte](1, 2, 3))
    eventually { counterEquals("received_bytes")(3) }

    client2.getOutputStream.write(1)
    eventually { counterEquals("received_bytes")(4) }

    Await.ready(server1.close(), 2.seconds)
    Await.ready(server2.close(), 2.seconds)
  }

  test("shuts down gracefully") {

    // turn off backpressure so that the three bytes
    // are recorded below.
    val c = new StatsCtx
    import c._

    val p = Params.empty + Label("test") + Stats(sr) + BackPressure(false)
    val server = serveService(_ => (), p, noopService)

    val (client1, client2, client3) = (new Socket(), new Socket(), new Socket())

    client1.connect(server.boundAddress)
    client2.connect(server.boundAddress)
    client3.connect(server.boundAddress)
    eventually { counterEquals("connects")(3) }

    // listening socket is closed
    Await.ready(server.close(Time.Top), Duration.fromSeconds(15))

    // new connection attempts fail
    intercept[java.net.ConnectException] { new Socket().connect(server.boundAddress) }

    // existing clients can still write
    client1.getOutputStream.write(1)
    client2.getOutputStream.write(1)
    client3.getOutputStream.write(1)

    eventually { counterEquals("received_bytes")(3) }
    Await.ready(server.close(), 2.seconds)
  }

  test("notices when the client cuts the connection") {
    val ctx = new StatsCtx
    import ctx._

    // we need to turn back-pressure off because netty doesn't notify you if the
    // client cut the connection unless you're reading or writing.  disabling
    // backpressure turns on autoread, so netty will alert us immediately that
    // the connection was cut.
    val params = Params.empty + Label("test") + Stats(sr) + BackPressure(false)

    @volatile var observedRequest: Option[String] = None

    object TestService extends Service[String, String] {
      val p = Promise[String]()
      @volatile var interrupted = false
      p.setInterruptHandler {
        case NonFatal(_) =>
          interrupted = true
      }

      def apply(request: String) = {
        observedRequest = Some(request)
        p
      }
    }

    val server = serveService(StringServerInit, params, TestService)

    val client = new Socket()
    eventually { client.connect(server.boundAddress) }
    client.getOutputStream.write("hello netty4!\n".getBytes("UTF-8"))
    client.getOutputStream.flush()

    eventually { assert(observedRequest == Some("hello netty4!")) }

    client.close()
    eventually { assert(TestService.interrupted) }

    Await.ready(server.close(), 2.seconds)
  }
}
