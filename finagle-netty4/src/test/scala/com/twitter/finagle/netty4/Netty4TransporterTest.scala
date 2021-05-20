package com.twitter.finagle.netty4

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Stack.Params
import com.twitter.finagle.{
  ConnectionFailedException,
  Failure,
  ProxyConnectException,
  ReadTimedOutException,
  Stack,
  WriteTimedOutException
}
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.netty4.decoder.TestFramer
import com.twitter.finagle.transport.{Transport, TransportContext}
import com.twitter.io.Buf
import com.twitter.util.{Await, Duration}
import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel._
import java.net.{InetAddress, InetSocketAddress, ServerSocket, Socket, SocketAddress}
import java.nio.channels.UnresolvedAddressException
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.funsuite.AnyFunSuite

class Netty4TransporterTest extends AnyFunSuite with Eventually with IntegrationPatience {
  val timeout = 15.seconds
  val frameSize = 4
  val data = "hello world"
  val defaultEnc = Buf.Utf8(_)
  val defaultDec = Buf.Utf8.unapply(_: Buf).getOrElse("???")

  val framer = () => new TestFramer(frameSize)

  private[this] class Ctx[A, B](
    transporterFn: (SocketAddress, Params) => Transporter[Buf, Buf, TransportContext],
    dec: Buf => B,
    enc: A => Buf) {
    var clientsideTransport: Transport[A, B] = null
    var server: ServerSocket = null
    var acceptedSocket: Socket = null

    def connect(): Unit = {
      server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress)
      val transporter = transporterFn(
        new InetSocketAddress(InetAddress.getLoopbackAddress, server.getLocalPort),
        Params.empty
      )
      val f = transporter().map(_.map(enc, dec))
      acceptedSocket = server.accept()
      clientsideTransport = Await.result(f, timeout)
    }
  }

  test("connection failures are propagated to the transporter promise") {
    val transporter =
      Netty4Transporter.framedBuf(Some(framer), new InetSocketAddress(0), Params.empty)

    val p = transporter()

    // connection failure is propagated to the Transporter promise
    val exc = intercept[Failure] {
      Await.result(p, Duration.fromSeconds(15))
    }

    exc match {
      case Failure(Some(e: ConnectionFailedException)) =>
        assert(e.getCause.isInstanceOf[java.net.SocketException])

      case other => fail(s"Expected ConnectionFailedException wrapped in a Failure, found $other")
    }

  }

  test("interrupts on read cut connections") {
    new Ctx(Netty4Transporter.framedBuf(Some(framer), _, _), defaultDec, defaultEnc) {
      connect()

      val read = clientsideTransport.read()

      assert(!server.isClosed)
      val expected = new Exception("boom!")
      read.raise(expected)
      val actual = intercept[Exception] {
        Await.result(read, 5.seconds)
      }
      assert(actual == expected)

      Await.result(clientsideTransport.onClose, 5.seconds)

      assert(acceptedSocket.getInputStream().read() == -1)
      server.close()
    }
  }

  test("Netty4ClientChannelInitializer produces a readable Transport") {
    new Ctx(Netty4Transporter.framedBuf(Some(framer), _, _), defaultDec, defaultEnc) {
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
        Await.result(clientsideTransport.read(), timeout) == data
          .drop(frameSize)
          .take(frameSize)
          .mkString
      )

      server.close()
    }
  }

  test("Netty4ClientChannelInitializer produces a writable Transport") {
    new Ctx(Netty4Transporter.framedBuf(Some(framer), _, _), defaultDec, defaultEnc) {
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

  test("end to end: asymmetric protocol") {
    val enc = { i: Int => Buf.ByteArray.Owned(Array(i.toByte)) }

    new Ctx(Netty4Transporter.framedBuf(Some(framer), _, _), defaultDec, enc) {
      connect()
      clientsideTransport.write(123)
      val serverInputStream = acceptedSocket.getInputStream
      assert(serverInputStream.read() == 123)

      acceptedSocket.getOutputStream.write("hello world".getBytes)

      assert(Await.result(clientsideTransport.read(), timeout) == "hell")
      assert(Await.result(clientsideTransport.read(), timeout) == "o wo")

      server.close()
    }
  }

  test("listener pipeline emits byte bufs with refCnt == 1") {
    val server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress)
    val transporter =
      Netty4Transporter.raw[ByteBuf, ByteBuf](
        { _: ChannelPipeline => () },
        new InetSocketAddress(InetAddress.getLoopbackAddress, server.getLocalPort),
        Params.empty)
    val transFuture =
      transporter()
    val acceptedSocket = server.accept()
    val clientsideTransport = Await.result(transFuture, timeout)

    val requestBytes = "hello world request".getBytes("UTF-8")
    val in = Unpooled.wrappedBuffer(requestBytes)
    clientsideTransport.write(in)

    val responseBytes = "some response".getBytes("UTF-8")
    acceptedSocket.getOutputStream.write(responseBytes)

    val responseBB = Await.result(clientsideTransport.read(), timeout)
    assert(responseBB.refCnt == 1)
  }

  test("Netty4ClientChannelInitializer pipelines enforce read timeouts") {
    @volatile var observedExn: Throwable = null
    val exnSnooper = new ChannelInboundHandlerAdapter {
      override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
        observedExn = cause
        super.exceptionCaught(ctx, cause)
      }
    }
    new Ctx(
      { (addr, params) =>
        Netty4Transporter.raw(
          { pipeline: ChannelPipeline => pipeline.addLast(exnSnooper) },
          addr,
          params + Transport.Liveness(readTimeout = 1.millisecond, Duration.Top, None))
      },
      defaultDec,
      defaultEnc
    ) {
      connect()

      intercept[ReadTimedOutException] {
        Await.result(clientsideTransport.read(), timeout)
      }
    }

    eventually {
      assert(observedExn.isInstanceOf[ReadTimedOutException])
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
      override def write(
        ctx: ChannelHandlerContext,
        msg: scala.Any,
        promise: ChannelPromise
      ): Unit =
        ()
    }

    new Ctx(
      { (addr, params) =>
        Netty4Transporter.raw(
          { pipeline: ChannelPipeline =>
            pipeline.addLast(exnSnooper)
            pipeline.addFirst(writeSwallower)
            ()
          },
          addr,
          params + Transport.Liveness(Duration.Top, writeTimeout = 1.millisecond, None)
        )
      },
      defaultDec,
      defaultEnc
    ) {
      connect()
      clientsideTransport.write("msg")
    }
    eventually {
      assert(observedExn.isInstanceOf[WriteTimedOutException])
    }
  }

  test("Respect non-retriable failures") {
    val fakeAddress = new InetSocketAddress(InetAddress.getLoopbackAddress, 50)

    def shouldNotBeWrapped(e: Exception): Unit = {
      val init: ChannelPipeline => Unit = { pipeline =>
        pipeline.addLast(new ChannelOutboundHandlerAdapter() {
          override def connect(
            ctx: ChannelHandlerContext,
            remote: SocketAddress,
            local: SocketAddress,
            promise: ChannelPromise
          ): Unit = promise.setFailure(e)
        })
      }

      val transporter = Netty4Transporter.raw[Unit, Unit](init, fakeAddress, Stack.Params.empty)
      assert(Await.result(transporter().liftToTry, 10.seconds).throwable == e)
    }

    shouldNotBeWrapped(new UnresolvedAddressException())
    shouldNotBeWrapped(new ProxyConnectException("boom", fakeAddress))
  }
}
