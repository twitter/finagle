package com.twitter.finagle.http2

import com.twitter.conversions.time._
import com.twitter.finagle.Stack
import com.twitter.finagle.transport.Transport
import com.twitter.io.{Buf, Reader}
import com.twitter.util.Await
import io.netty.handler.codec.http.{DefaultFullHttpRequest, HttpVersion, HttpMethod}
import java.net.ServerSocket
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite

@RunWith(classOf[JUnitRunner])
class Http2TransporterTest extends FunSuite {
  def evaluate(fn: (Reader, Transport[Any, Any]) => Unit): Unit = {
    val transporter = Http2Transporter(Stack.Params.empty)
    val unbound = new ServerSocket(0)

    val fTransport = transporter(unbound.getLocalSocketAddress)
    val socket = unbound.accept()

    val transport = Await.result(fTransport, 5.seconds)
    val reader = Reader.fromStream(socket.getInputStream)

    fn(reader, transport)

    reader.discard()
    Await.result(transport.close(), 5.seconds)
  }

  ignore("sends a regular upgrade") {
    evaluate { case (reader, transport) =>
      val req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "http://")
      val written = transport.write(req)
      val Buf.Utf8(string) = Await.result(reader.read(Int.MaxValue), 5.seconds).get

      // the cleartext upgrade is a regular http request with the upgrade: h2c header.
      // we also send compressed settings, so that the remote server knows how the upgrade should
      // proceed.
      // the stream id is an implementation detail, and we'll remove it in the future.
      val expected = """GET http:/// HTTP/1.1
       |x-http2-stream-id: 1
       |upgrade: h2c
       |HTTP2-Settings: AAEAABAAAAIAAAABAAN_____AAQAAP__AAUAAEAAAAZ_____
       |connection: HTTP2-Settings,upgrade
       |
       |""".stripMargin.replaceAll("\n", "\r\n")
      assert(string == expected)
      assert(written.isDefined)
    }
  }

  ignore("sends a regular upgrade with headers") {
    evaluate { case (reader, transport) =>
      val req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "http://")
      req.headers.add("bleh", "OK")
      val written = transport.write(req)
      val Buf.Utf8(string) = Await.result(reader.read(Int.MaxValue), 5.seconds).get

      // the cleartext upgrade is a regular http request with the upgrade: h2c header.
      // we also send compressed settings, so that the remote server knows how the upgrade should
      // proceed.
      // the stream id is an implementation detail, and we'll remove it in the future.
      val expected = """GET http:/// HTTP/1.1
                       |bleh: OK
                       |x-http2-stream-id: 1
                       |upgrade: h2c
                       |HTTP2-Settings: AAEAABAAAAIAAAABAAN_____AAQAAP__AAUAAEAAAAZ_____
                       |connection: HTTP2-Settings,upgrade
                       |
                       |""".stripMargin.replaceAll("\n", "\r\n")
      assert(string == expected)
      assert(written.isDefined)
    }
  }

  ignore("sends a regular upgrade with long headers") {
    evaluate { case (reader, transport) =>
      val req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "http://")
      val manyAs = "a" * 8192
      req.headers.add("bleh", manyAs)
      val written = transport.write(req)
      val Buf.Utf8(chunk1) = Await.result(reader.read(Int.MaxValue), 5.seconds).get
      val Buf.Utf8(chunk2) = Await.result(reader.read(Int.MaxValue), 5.seconds).get
      val Buf.Utf8(chunk3) = Await.result(reader.read(Int.MaxValue), 5.seconds).get

      // the cleartext upgrade is a regular http request with the upgrade: h2c header.
      // we also send compressed settings, so that the remote server knows how the upgrade should
      // proceed.
      // the stream id is an implementation detail, and we'll remove it in the future.
      val expected = s"""GET http:/// HTTP/1.1
                       |bleh: $manyAs
                       |x-http2-stream-id: 1
                       |upgrade: h2c
                       |HTTP2-Settings: AAEAABAAAAIAAAABAAN_____AAQAAP__AAUAAEAAAAZ_____
                       |connection: HTTP2-Settings,upgrade
                       |
                       |""".stripMargin.replaceAll("\n", "\r\n")
      assert(chunk1 ++ chunk2 ++ chunk3 == expected)
      assert(written.isDefined)
    }
  }
}
