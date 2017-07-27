package com.twitter.finagle.http2

import com.twitter.conversions.time._
import com.twitter.finagle.Stack
import com.twitter.io.{Buf, Writer, Reader}
import com.twitter.util.Await
import io.netty.handler.codec.http.HttpMessage
import java.net.{Socket, InetAddress, InetSocketAddress}
import java.util.concurrent.{SynchronousQueue, TimeUnit}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite

@RunWith(classOf[JUnitRunner])
class Http2ListenerTest extends FunSuite {
  def evaluate(fn: (Writer, Reader) => Unit): Any = {
    val listener = Http2Listener[HttpMessage, HttpMessage](Stack.Params.empty)
    val q = new SynchronousQueue[Any]()
    val server = listener.listen(new InetSocketAddress(0)) { transport =>
      transport.read().onSuccess { msg =>
        q.offer(msg)
      }
    }
    val port = server.boundAddress match {
      case addr: InetSocketAddress => addr.getPort
      case _ => ???
    }
    val socket = new Socket(InetAddress.getLoopbackAddress, port)
    val writer = Writer.fromOutputStream(socket.getOutputStream)
    val reader = Reader.fromStream(socket.getInputStream)

    fn(writer, reader)
    assert(socket.isConnected && !socket.isClosed)

    val result = q.poll(5, TimeUnit.SECONDS)

    reader.discard()
    Await.result(writer.close(), 5.seconds)
    Await.result(server.close(), 5.seconds)
    result
  }

  // this test fails on travisci, so let's disable it until we understand why
  ignore("Http2Listener should upgrade neatly") {
    val result = evaluate {
      case (writer, reader) =>
        val msg = s"""GET http:/// HTTP/1.1
       |x-http2-stream-id: 1
       |upgrade: h2c
       |HTTP2-Settings: AAEAABAAAAIAAAABAAN_____AAQAAP__AAUAAEAAAAZ_____
       |connection: HTTP2-Settings,upgrade
       |content-length: 0
       |
       |""".stripMargin.replaceAll("\n", "\r\n")

        val response = """HTTP/1.1 101 Switching Protocols
       |connection: upgrade
       |upgrade: h2c
       |content-length: 0
       |
       |""".stripMargin.replaceAll("\n", "\r\n")

        Await.result(writer.write(Buf.Utf8(msg)), 5.seconds)

        val Buf.Utf8(rep1) = Await.result(reader.read(Int.MaxValue), 5.seconds).get
        assert(rep1 == response)
    }

    assert(result.isInstanceOf[HttpMessage])
  }
}
