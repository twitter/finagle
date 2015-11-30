package com.twitter.finagle.netty4


import com.twitter.finagle.Stack.Params
import com.twitter.finagle.dispatch.SerialServerDispatcher
//import com.twitter.finagle.netty4.transport.SocketChannelTransport CSL-2050
import com.twitter.finagle.param.{Stats, Label}
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.{Service, Status}
import com.twitter.io.Charsets
import com.twitter.util._
import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelPipeline
import io.netty.channel.socket.SocketChannel
import io.netty.handler.codec.string.{StringDecoder, StringEncoder}
import io.netty.handler.codec.{Delimiters, DelimiterBasedFrameDecoder}
import java.net.{SocketAddress, InetAddress, Socket, InetSocketAddress}
import java.security.cert.Certificate
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.junit.JUnitRunner
import scala.collection.JavaConverters._


@RunWith(classOf[JUnitRunner])
class Netty4ListenerTest extends FunSuite with Eventually with IntegrationPatience {

  // a Transport whose reads and writes never complete
  private[netty4] class NullTransport[In,Out] extends Transport[In, Out] {
    val closeP = new Promise[Throwable]
    val readP = Future.never
    val writeP = Future.never

    var _status: Status = Status.Open

    def write(req: In): Future[Unit] = writeP
    def remoteAddress: SocketAddress = throw new NotImplementedError()
    def peerCertificate: Option[Certificate] = None
    def localAddress: SocketAddress = throw new NotImplementedError()
    def status: Status = _status
    def read(): Future[Out] = ???
    val onClose: Future[Throwable] = closeP
    def close(deadline: Time): Future[Unit] = {
      _status = Status.Closed
      closeP.setValue(new Exception("closed channelExn"))
      Future.Done
    }
  }

  // the /dev/null of dispatchers
  val nopDispatch = { _: Transport[ByteBuf, ByteBuf] => () }

  private[this] trait StatsCtx {
    val sr: InMemoryStatsReceiver = new InMemoryStatsReceiver

    def statEquals(name: String*)(expected: Float*): Unit =
      assert(sr.stat(name: _*)() == expected)

    def counterEquals(name: String*)(expected: Int): Unit =
      assert(sr.counters(name) == expected)
  }

  private[this] trait Ctx extends StatsCtx {
    val p = Params.empty + Label("test") + Stats(sr)
    val listener = Netty4Listener[ByteBuf, ByteBuf](
      p,
      transportFactory = { _: SocketChannel => new NullTransport }
    )
  }


  test("frames pipeline messages and bridges transports and service dispatchers (aka it works end-to-end)") {
    val ctx = new StatsCtx { }
    import ctx._

    object StringServerInit extends (ChannelPipeline => Unit) {
      def apply(pipeline: ChannelPipeline): Unit = {
        pipeline.addLast("line", new DelimiterBasedFrameDecoder(100, Delimiters.lineDelimiter(): _*))
        pipeline.addLast("stringDecoder", new StringDecoder(Charsets.Utf8))
        pipeline.addLast("stringEncoder", new StringEncoder(Charsets.Utf8))
      }
    }

    val p = Params.empty + Label("test") + Stats(sr) + PipelineInit(StringServerInit)
    val listener = Netty4Listener[String, String](p)

    @volatile var observedRequest: Option[String] = None

    val service = new Service[String, String] {
      def apply(request: String) = {
        observedRequest = Some(request)
        Future.value("hi2u")
      }
    }

    val serveTransport = (t: Transport[String, String]) => new SerialServerDispatcher(t, service)
    val server = listener.listen(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))(serveTransport(_))

    val client = new Socket()
    eventually { client.connect(server.boundAddress) }
    client.getOutputStream.write("hello netty4!\n".getBytes("UTF-8"))
    client.getOutputStream.flush()

    eventually { assert(observedRequest == Some("hello netty4!")) }

    val response = client.getInputStream
    val expected = "hi2u"
    val actual = new String(Array.fill("hi2u".length)(response.read().toByte))
    assert(actual == expected)
    server.close()
  }

  test("Netty4Listener records basic channel stats") {
    val ctx = new StatsCtx { }
    import ctx._

    val p = Params.empty + Label("srv") + Stats(sr)
    val listener = Netty4Listener[ByteBuf, ByteBuf](
        p,
        transportFactory = { _: SocketChannel => new NullTransport }
      )
    val server1 = listener.listen(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))(nopDispatch)
    val server2 = listener.listen(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))(nopDispatch)

    val (client1, client2) = (new Socket(), new Socket())

    eventually { client1.connect(server1.boundAddress) }
    eventually { client2.connect(server2.boundAddress) }

    eventually { counterEquals("connects")(2) }

    client1.getOutputStream.write(Array[Byte](1, 2, 3))
    eventually { counterEquals("received_bytes")(3) }

    client2.getOutputStream.write(1)
    eventually { counterEquals("received_bytes")(4) }

    server1.close()
    server2.close()
  }

  test("Netty4Listener shuts down gracefully") {
    val c = new Ctx {}
    import c._

    val serverAddr = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
    val server = listener.listen(serverAddr)(nopDispatch)

    val (client1, client2, client3) = (new Socket(), new Socket(), new Socket())

    client1.connect(server.boundAddress)
    client2.connect(server.boundAddress)
    client3.connect(server.boundAddress)
    eventually { counterEquals("connects")(3) }

    // listening socket is closed
    Await.ready(server.close(), Duration.fromSeconds(15))

    // new connection attempts fail
    intercept[java.net.ConnectException] { new Socket().connect(server.boundAddress) }

    // existing clients can still write
    client1.getOutputStream.write(1)
    client2.getOutputStream.write(1)
    client3.getOutputStream.write(1)

    eventually { counterEquals("received_bytes")(3) }
  }
}
