package com.twitter.finagle.http2

import com.twitter.concurrent.AsyncQueue
import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Stack
import com.twitter.io.{Buf, Writer, Reader}
import com.twitter.util._
import io.netty.handler.codec.http.{HttpMessage, HttpRequest}
import java.net.{Socket, InetAddress, InetSocketAddress}
import org.scalatest.funsuite.AnyFunSuite

private object Http2ListenerTest {
  def await[A](f: Future[A], to: Duration = 5.seconds) = Await.result(f, to)

  class Ctx {
    val recvdByServer = new AsyncQueue[Any]

    private[this] val server = {
      Http2Listener[HttpMessage, HttpMessage](Stack.Params.empty)
        .listen(new InetSocketAddress(0)) { transport =>
          transport.read().respond {
            case Return(m) => recvdByServer.offer(m)
            case Throw(exc) => recvdByServer.fail(exc)
          }
        }
    }

    private[this] val (writer, reader) = {
      val port = server.boundAddress.asInstanceOf[InetSocketAddress].getPort
      val socket = new Socket(InetAddress.getLoopbackAddress, port)
      (
        Writer.fromOutputStream(socket.getOutputStream),
        Reader.fromStream(socket.getInputStream)
      )
    }

    def write(message: String): Future[Unit] =
      writer.write(Buf.Utf8(message))

    def read(): Future[Option[String]] =
      reader
        .read().map(_.map {
          case Buf.Utf8(message) => message
        })

    def close(): Future[Unit] = {
      reader.discard()
      Closable.all(writer, server).close()
    }
  }
}

class Http2ListenerTest extends AnyFunSuite {
  import Http2ListenerTest._

  test("Http2Listener should upgrade neatly")(new Ctx {
    await(write("""GET http:/// HTTP/1.1
      |x-http2-stream-id: 1
      |upgrade: h2c
      |HTTP2-Settings: AAEAABAAAAIAAAABAAN_____AAQAAP__AAUAAEAAAAZ_____
      |connection: HTTP2-Settings,upgrade
      |content-length: 0
      |x-hello: world
      |
      |""".stripMargin.replaceAll("\n", "\r\n")))

    assert(await(read()).get == """HTTP/1.1 101 Switching Protocols
      |connection: upgrade
      |upgrade: h2c
      |
      |""".stripMargin.replaceAll("\n", "\r\n"))

    val req = await(recvdByServer.poll()).asInstanceOf[HttpRequest]
    assert(req.headers.get("x-hello") == "world")
    await(close())
  })

  test("Http2Listener should not upgrade with an invalid URI (non-ASCII)")(new Ctx {

    await(write(s"""GET http:///DSC02175拷貝.jpg HTTP/1.1
                  |x-http2-stream-id: 1
                  |upgrade: h2c
                  |HTTP2-Settings: AAEAABAAAAIAAAABAAN_____AAQAAP__AAUAAEAAAAZ_____
                  |connection: HTTP2-Settings,upgrade
                  |content-length: 0
                  |x-hello: world
                  |
                  |""".stripMargin.replaceAll("\n", "\r\n")))

    assert(await(read()).get == """HTTP/1.0 400 Bad Request
                                  |Connection: close
                                  |Content-Length: 0
                                  |
                                  |""".stripMargin.replaceAll("\n", "\r\n"))

    await(close())
  })

  test("Http2Listener should not upgrade with an invalid URI (encoding)")(new Ctx {

    await(write(s"""GET http:///1%%.jpg HTTP/1.1
                   |x-http2-stream-id: 1
                   |upgrade: h2c
                   |HTTP2-Settings: AAEAABAAAAIAAAABAAN_____AAQAAP__AAUAAEAAAAZ_____
                   |connection: HTTP2-Settings,upgrade
                   |content-length: 0
                   |x-hello: world
                   |
                   |""".stripMargin.replaceAll("\n", "\r\n")))

    assert(await(read()).get == """HTTP/1.0 400 Bad Request
                                  |Connection: close
                                  |Content-Length: 0
                                  |
                                  |""".stripMargin.replaceAll("\n", "\r\n"))

    await(close())
  })
}
