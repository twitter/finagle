package com.twitter.finagle.http

import java.io.{ByteArrayOutputStream, OutputStream, PrintStream}
import java.net.InetSocketAddress
import java.util.zip.{DeflaterOutputStream, GZIPOutputStream}
import com.twitter.conversions.time._
import com.twitter.finagle
import com.twitter.finagle.Service
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.io.Buf
import com.twitter.util.{Await, Closable, Future}
import org.jboss.netty.handler.codec.http.HttpHeaders
import org.junit.runner.RunWith
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.junit.JUnitRunner
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/** Provides tests for server side content decoding.
  *
  * If and when client side compression is implemented, this test should probably be removed in favour of a complete
  * entry in [[EndToEndTest]]. Client side compression is currently made problematic by netty
  * (see https://github.com/netty/netty/issues/4970).
  */
@RunWith(classOf[JUnitRunner])
class ServerSideDecodingTest extends FunSuite with GeneratorDrivenPropertyChecks with BeforeAndAfterAll {
  // Echo server (with decoding)
  val server = finagle.Http.server
    .withLabel("server")
    .withDecompression(true)
    .serve("localhost:*", new Service[Request, Response] {
      def apply(request: Request) = {
        val response = Response()
        response.contentString = request.contentString
        Future.value(response)
      }
    })

  // Standard client
  val client = ClientBuilder()
    .codec(Http())
    .hosts(Seq(server.boundAddress.asInstanceOf[InetSocketAddress]))
    .hostConnectionLimit(1)
    .build()

  // URL at which all requests should be made
  val url: String = {
    val adr = server.boundAddress.asInstanceOf[InetSocketAddress]
    s"http://${adr.getHostName}:${adr.getPort}/"
  }

  // Makes sure everything is cleaned up
  override def afterAll(): Unit = {
    Closable.all(client, server)
  }

  // Helper class - might be overkill to have a sum type for just one test, but it makes it simple to provide an
  // Arbitrary instance for encoders and to make the actual test that much more readable.
  sealed abstract class Encoder(val name: String) {
    def encodeWith(out: OutputStream): OutputStream
    def encode(string: String): Buf = {
      val bytes = new ByteArrayOutputStream()
      val out = new PrintStream(encodeWith(bytes), true, "UTF-8")
      out.print(string)
      out.close() // Do not remove, filter streams absolutely need this to generate legal content.
      Buf.ByteArray.Shared(bytes.toByteArray)
    }
  }

  case object Gzip extends Encoder(HttpHeaders.Values.GZIP) {
    override def encodeWith(out: OutputStream) = new GZIPOutputStream(out)
  }

  case object Deflate extends Encoder(HttpHeaders.Values.DEFLATE) {
    override def encodeWith(out: OutputStream) = new DeflaterOutputStream(out)
  }

  case object Identity extends Encoder(HttpHeaders.Values.IDENTITY) {
    override def encodeWith(out: OutputStream) = out
  }

  implicit val arbEncoder: Arbitrary[Encoder] = Arbitrary(Gen.oneOf(Gzip, Deflate, Identity))

  test("decode client-side encoded entity bodies") {
    forAll { (content: String, encoder: Encoder) ⇒
      val req = RequestBuilder()
        .setHeader("Content-Encoding", encoder.name)
        .setHeader("Content-Type", "text/plain;charset=utf-8")
        .url(url)

      assert(content == Await.result(client(req.buildPost(encoder.encode(content))), 1.second).contentString)
    }
  }
}
