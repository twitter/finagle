package com.twitter.finagle.netty4

import com.twitter.conversions.time._
import com.twitter.finagle.Stack.Params
import com.twitter.finagle.WriteTimedOutException
import com.twitter.finagle.codec.{FrameEncoder, FixedLengthDecoder}
import com.twitter.finagle.transport.Transport
import com.twitter.io.Buf
import com.twitter.util.{Await, Duration, Promise}
import io.netty.buffer.{Unpooled, ByteBuf}
import io.netty.channel._
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.timeout.ReadTimeoutException
import java.net.{Socket, InetSocketAddress, InetAddress, ServerSocket}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class Netty4ClientChannelInitializerTest
  extends FunSuite
  with Eventually
  with IntegrationPatience {

  val timeout = Duration.fromSeconds(15)

  private[this] trait Ctx {
    val frameSize = 4
    val data = "hello world"
    val client: SocketChannel = new NioSocketChannel()
    val loop = new NioEventLoopGroup()
    loop.register(client)
    val enc = Buf.Utf8(_)
    def dec = new FixedLengthDecoder(frameSize, Buf.Utf8.unapply(_).getOrElse("????"))
    def customHandlers: Seq[ChannelHandler] = Nil
    def params = Params.empty

    var channelInit: Netty4ClientChannelInitializer[String, String] = null

    var server: ServerSocket = null
    var acceptedSocket: Socket = null
    var clientsideTransport: Transport[String, String] = null
    val transportP = new Promise[Transport[String, String]]

    def initChannel() = {
      channelInit =
        new Netty4ClientChannelInitializer(
          transportP,
          params,
          Some(enc),
          Some(() => dec)
        )
      server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress)
      channelInit.initChannel(client)
    }

    def connect() = {

      client.connect(
        new InetSocketAddress(InetAddress.getLoopbackAddress, server.getLocalPort)
      ).awaitUninterruptibly(timeout.inMilliseconds)

      acceptedSocket = server.accept()
      clientsideTransport = Await.result(transportP, timeout)
    }
  }

  test("Netty4ClientChannelInitializer produces a readable Transport") {
    new Ctx {
      initChannel()
      connect()

      val os = acceptedSocket.getOutputStream
      os.write(data.getBytes("UTF-8"))
      os.flush()
      os.close()

      // one server message produces two client transport messages
      assert(
        Await.result(clientsideTransport.read(), timeout) == data.take(frameSize).mkString
      )
      assert(
        Await.result(clientsideTransport.read(), timeout) == data.drop(frameSize).take(frameSize).mkString
      )

      server.close()
    }
  }

  test("Netty4ClientChannelInitializer produces a writable Transport") {
    new Ctx {
      initChannel()
      connect()

      Await.ready(clientsideTransport.write(data), timeout)

      val bytes = new Array[Byte](data.length)
      val is = acceptedSocket.getInputStream
      is.read(bytes, 0, bytes.length)
      assert(new String(bytes) == data)

      is.close()
      server.close()
    }
  }

  test("Netty4ClientChannelInitializer pipelines enforce read timeouts") {
    @volatile var observedExn: Throwable = null
    val exnSnooper = new ChannelInboundHandlerAdapter {
      override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
        observedExn = cause
        super.exceptionCaught(ctx, cause)
      }
    }
    new Ctx {
      override def params =
        Params.empty + Transport.Liveness(readTimeout = 1.millisecond, Duration.Top, None)

      initChannel()
      client.pipeline.addLast(exnSnooper)
      connect()
    }

    eventually {
      assert(observedExn.isInstanceOf[ReadTimeoutException])
    }
  }

  test("Netty4ClientChannelInitializer pipelines enforce write timeouts") {
    @volatile var observedExn: Throwable = null
    val exnSnooper = new ChannelInboundHandlerAdapter {
      override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
        observedExn = cause
        super.exceptionCaught(ctx, cause)
      }
    }

    val writeSwallower = new ChannelOutboundHandlerAdapter {
      override def write(ctx: ChannelHandlerContext, msg: scala.Any, promise: ChannelPromise): Unit =
        ()
    }

    new Ctx {
      override def params =
        Params.empty + Transport.Liveness(Duration.Top, writeTimeout = 1.millisecond, None)

      initChannel()
      client.pipeline.addLast (exnSnooper)
      client.pipeline.addFirst (writeSwallower)
      connect()
      client.pipeline.write ("msg")

    }
    eventually {
      assert(observedExn.isInstanceOf[WriteTimedOutException])
    }
  }

  test("end to end: asymmetric protocol") {
    val ctx = new Ctx {}
    val enc: FrameEncoder[Int] = { i: Int => Buf.ByteArray.Owned(Array(i.toByte)) }
    val decoder =
      new FixedLengthDecoder[String](4, Buf.Utf8.unapply(_).getOrElse("???"))

    val transportP = new Promise[Transport[Int, String]]
    val channelInit =
      new Netty4ClientChannelInitializer[Int, String](
        transportP,
        Params.empty,
        Some(enc),
        Some(() => decoder)
      )

    channelInit.initChannel(ctx.client)
    val server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress)
    ctx.client.connect(
      new InetSocketAddress(InetAddress.getLoopbackAddress, server.getLocalPort)
    ).awaitUninterruptibly(timeout.inMilliseconds)

    val acceptedSocket = server.accept()
    val transport: Transport[Int, String] = Await.result(transportP, timeout)
    transport.write(123)
    val serverInputStream = acceptedSocket.getInputStream
    assert(serverInputStream.read() == 123)

    acceptedSocket.getOutputStream.write("hello world".getBytes)

    assert(Await.result(transport.read(), timeout) == "hell")
    assert(Await.result(transport.read(), timeout) == "o wo")
  }

  test("raw channel initializer exposes netty pipeline") {
    val p = new Promise[Transport[ByteBuf, ByteBuf]]
    val reverser = new ChannelOutboundHandlerAdapter{
      override def write(ctx: ChannelHandlerContext, msg: scala.Any, promise: ChannelPromise): Unit = msg match {
        case b: ByteBuf =>
          val bytes = new Array[Byte](b.readableBytes)
          b.readBytes(bytes)
          val reversed = Unpooled.wrappedBuffer(bytes.reverse)
          super.write(ctx, reversed, promise)
        case _ => fail("expected ByteBuf message")
      }
    }
    val init =
      new RawNetty4ClientChannelInitializer[ByteBuf, ByteBuf](p, Params.empty, _.addLast(reverser))

    val channel: SocketChannel = new NioSocketChannel()
    val loop = new NioEventLoopGroup()
    loop.register(channel)
    init.initChannel(channel)

    val msgSeen = new Promise[ByteBuf]
    channel.pipeline.addFirst(new ChannelOutboundHandlerAdapter {
      override def write(ctx: ChannelHandlerContext, msg: scala.Any, promise: ChannelPromise): Unit = msg match {
        case b: ByteBuf => msgSeen.setValue(b)
        case _ => fail("expected ByteBuf message")
      }
    })
    val bytes = Array(1.toByte, 2.toByte, 3.toByte)
    channel.write(Unpooled.wrappedBuffer(bytes))

    val seen = new Array[Byte](3)
    Await.result(msgSeen, 5.seconds).readBytes(seen)
    assert(seen.toList == bytes.reverse.toList)
  }
}