package com.twitter.finagle.netty4.pushsession

import com.twitter.concurrent.AsyncQueue
import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Stack.Params
import com.twitter.finagle._
import com.twitter.finagle.pushsession.{PushChannelHandle, PushSession, PushTransporter}
import com.twitter.finagle.netty4.codec.BufCodec
import com.twitter.finagle.netty4.decoder.{DecoderHandler, TestFramer}
import com.twitter.finagle.transport.Transport
import com.twitter.io.Buf
import com.twitter.util._
import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel.{
  ChannelHandlerContext,
  ChannelInboundHandlerAdapter,
  ChannelOutboundHandlerAdapter,
  ChannelPipeline,
  ChannelPromise
}
import io.netty.handler.codec.MessageToMessageCodec
import java.net.{InetAddress, InetSocketAddress, ServerSocket, Socket, SocketAddress}
import java.nio.channels.UnresolvedAddressException
import java.nio.charset.StandardCharsets
import java.util
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.funsuite.AnyFunSuite

class Netty4PushTransporterTest extends AnyFunSuite with Eventually with IntegrationPatience {
  private val frameSize = 4
  private val data = "hello world"

  private def await[T](result: Awaitable[T]): T = Await.result(result, 15.seconds)

  // converts to string frames of 4 bytes/chars each (we're using ASCII chars for tests which are 1 byte each)
  private def withStringFramer(pipeline: ChannelPipeline): Unit = {
    class BufToStringCodec extends MessageToMessageCodec[Buf, String] {
      def encode(ctx: ChannelHandlerContext, msg: String, out: util.List[AnyRef]): Unit =
        out.add(Buf.Utf8(msg))

      def decode(ctx: ChannelHandlerContext, msg: Buf, out: util.List[AnyRef]): Unit =
        out.add(Buf.Utf8.unapply(msg).getOrElse("???"))
    }

    pipeline.addLast(BufCodec.Key, BufCodec)
    pipeline.addLast("framer", new DecoderHandler(new TestFramer(frameSize)))
    pipeline.addLast("transcoder", new BufToStringCodec)
  }

  class TestSession[In, Out](handle: PushChannelHandle[In, Out])
      extends PushSession[In, Out](handle) {
    def status: Status = handle.status
    def close(deadline: Time): Future[Unit] = handle.close(deadline)

    def onClose: Future[Unit] = handle.onClose

    private[this] val receivedData = new AsyncQueue[In]()

    def receive(message: In): Unit =
      receivedData.offer(message)

    def write(msg: Out): Future[Unit] = {
      val p = Promise[Unit]
      handle.serialExecutor.execute(new Runnable {
        def run(): Unit = {
          handle.send(msg)(p.updateIfEmpty(_))
        }
      })
      p
    }

    def readFuture(): Future[In] = receivedData.poll()
  }

  private[this] class Ctx[In, Out](
    transporterFn: (SocketAddress, Params) => PushTransporter[In, Out]) {

    var clientsideTransport: TestSession[In, Out] = null
    var server: ServerSocket = null
    var acceptedSocket: Socket = null

    protected def makeSession(handle: PushChannelHandle[In, Out]): Future[TestSession[In, Out]] = {
      Future.value(new TestSession[In, Out](handle))
    }

    def connect(): Unit = {
      server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress)
      val transporter = transporterFn(
        new InetSocketAddress(InetAddress.getLoopbackAddress, server.getLocalPort),
        Params.empty
      )
      val f = transporter(makeSession)

      acceptedSocket = server.accept()

      clientsideTransport = await(f)
    }

    def closeCtx(): Unit = {
      if (server != null) server.close()
      if (acceptedSocket != null) acceptedSocket.close()
      if (clientsideTransport != null) clientsideTransport.close()
    }
  }

  test("connection failures are propagated to the transporter promise") {
    val transporter = Netty4PushTransporter.raw(_ => (), new InetSocketAddress(0), Params.empty)

    val p = transporter(_ => Future.exception(new Exception("Shouldn't get here!")))

    // connection failure is propagated to the Transporter promise
    val exc = intercept[Failure] {
      await(p)
    }

    exc match {
      case Failure(Some(e: ConnectionFailedException)) =>
        assert(e.getCause.isInstanceOf[java.net.SocketException])

      case other => fail(s"Expected ConnectionFailedException wrapped in a Failure, found $other")
    }

  }

  test("Netty4ClientChannelInitializer produces a readable Transport") {
    new Ctx(Netty4PushTransporter.raw[String, String](withStringFramer, _, _)) {
      connect()

      val os = acceptedSocket.getOutputStream
      os.write(data.getBytes(StandardCharsets.UTF_8))
      os.flush()
      os.close()

      // one server message produces two client transport messages
      assert(
        await(clientsideTransport.readFuture()) == data.take(frameSize).mkString
      )
      assert(
        await(clientsideTransport.readFuture()) == data
          .drop(frameSize)
          .take(frameSize)
          .mkString
      )

      closeCtx()
    }
  }

  test("Netty4ClientChannelInitializer produces a writable Transport") {
    new Ctx(Netty4PushTransporter.raw[String, String](withStringFramer, _, _)) {
      connect()

      await(clientsideTransport.write(data))

      val bytes = new Array[Byte](data.length)
      val is = acceptedSocket.getInputStream
      is.read(bytes, 0, bytes.length)
      assert(new String(bytes) == data)

      is.close()
      closeCtx()
    }
  }

  test("listener pipeline emits byte bufs with refCnt == 1") {
    val server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress)
    val transporter =
      Netty4PushTransporter.raw[ByteBuf, ByteBuf](
        _ => (),
        new InetSocketAddress(InetAddress.getLoopbackAddress, server.getLocalPort),
        Params.empty
      )
    val transFuture = transporter(h => Future.value(new TestSession[ByteBuf, ByteBuf](h)))
    val acceptedSocket = server.accept()
    val clientsideTransport = await(transFuture)

    val requestBytes = "hello world request".getBytes("UTF-8")
    val in = Unpooled.wrappedBuffer(requestBytes)
    clientsideTransport.write(in)

    val responseBytes = "some response".getBytes("UTF-8")
    acceptedSocket.getOutputStream.write(responseBytes)
    acceptedSocket.getOutputStream.flush()

    val responseBB = await(clientsideTransport.readFuture())
    assert(responseBB.refCnt == 1)
  }

  test("Netty4ClientChannelInitializer pipelines enforce read timeouts") {
    new Ctx(
      { (addr, params) =>
        Netty4PushTransporter.raw[String, String](
          withStringFramer,
          addr,
          params + Transport.Liveness(readTimeout = 1.second, Duration.Top, None)
        )
      }
    ) {
      connect()

      intercept[ReadTimedOutException] {
        await(clientsideTransport.onClose)
      }

      closeCtx()
    }
  }

  test("Failure before session resolution") {
    val ex = new Exception("sadface")
    object FailingHandler extends ChannelInboundHandlerAdapter {
      @volatile
      private[this] var ctx: ChannelHandlerContext = null

      val latch = Promise[Unit]

      latch.onSuccess { _ => if (ctx != null) ctx.fireExceptionCaught(ex) }

      override def handlerAdded(ctx: ChannelHandlerContext): Unit = {
        this.ctx = ctx
      }
    }

    new Ctx(
      { (addr, params) =>
        new Netty4PushTransporter[String, String](
          _.addLast(FailingHandler),
          withStringFramer,
          addr,
          params
        )
      }
    ) {

      override protected def makeSession(
        handle: PushChannelHandle[String, String]
      ): Future[TestSession[String, String]] = {
        val p = Promise[TestSession[String, String]]()
        // We don't resolve the session until the handle closes due to the exception
        handle.onClose.ensure {
          p.updateIfEmpty(Return(new TestSession[String, String](handle)))
        }

        FailingHandler.latch.setDone()

        p
      }

      connect()

      val observed = intercept[UnknownChannelException] {
        await(clientsideTransport.onClose)
      }
      assert(observed.getCause == ex)

      closeCtx()
    }
  }

  test("Netty4ClientChannelInitializer pipelines enforce write timeouts") {

    val writeSwallower = new ChannelOutboundHandlerAdapter {
      override def write(
        ctx: ChannelHandlerContext,
        msg: scala.Any,
        promise: ChannelPromise
      ): Unit =
        ()
    }

    new Ctx({ (addr, params) =>
      Netty4PushTransporter.raw[String, String](
        { pipeline: ChannelPipeline =>
          withStringFramer(pipeline)
          pipeline.addFirst(writeSwallower)
          ()
        },
        addr,
        params + Transport.Liveness(Duration.Top, writeTimeout = 1.millisecond, None)
      )
    }) {
      connect()

      // We don't await on this since we discarded it's associated ChannelPromise in writeSwallower
      clientsideTransport.write("msg")

      intercept[WriteTimedOutException] {
        await(clientsideTransport.onClose)
      }

      closeCtx()
    }
  }

  test("Respect non-retriable failures") {
    val fakeAddress = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)

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

      val transporter =
        new Netty4PushTransporter[Unit, Unit](init, _ => (), fakeAddress, Stack.Params.empty)
      assert(await(transporter(_ => ???).liftToTry).throwable == e)
    }

    shouldNotBeWrapped(new UnresolvedAddressException())
    shouldNotBeWrapped(new ProxyConnectException("boom", fakeAddress))
  }
}
