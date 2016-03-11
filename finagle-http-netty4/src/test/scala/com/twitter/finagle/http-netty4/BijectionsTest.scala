package com.twitter.finagle.http4

import com.twitter.finagle.http._
import com.twitter.io.{BufReader, Buf}
import io.netty.buffer.Unpooled
import io.netty.handler.codec.http._
import java.net.URI
import java.nio.charset.StandardCharsets.UTF_8
import org.junit.runner.RunWith
import org.scalacheck.{Gen, Arbitrary}
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import scala.collection.JavaConverters._
import scala.util.Random

object BijectionsTest {
  import Arbitrary.arbitrary

  val arbMethod = Gen.oneOf(
    Method.Get, Method.Post, Method.Trace, Method.Delete, Method.Put,
    Method.Connect, Method.Options)

  val arbKeys = Gen.oneOf("Foo", "Bar", "Foo-Bar", "Bar-Baz")

  val arbUri = for {
    scheme  <- Gen.oneOf("http", "https")
    hostLen <- Gen.choose(1,20)
    pathLen <- Gen.choose(1,20)
    tld     <- Gen.oneOf(".net",".com", "org", ".edu")
    host = Random.alphanumeric.take(hostLen).mkString
    path = Random.alphanumeric.take(pathLen).mkString
  } yield (new URI(scheme, host + tld, "/" + path, null)).toASCIIString

  val arbHeader = for {
    key <- arbKeys
    len <- Gen.choose(0, 10)
  } yield (key, Random.alphanumeric.take(len).mkString)

  val arbResponse = for {
    code    <- Gen.chooseNum(100, 510)
    version <- Gen.oneOf(Version.Http10, Version.Http11)
    headers <- Gen.containerOf[Seq, (String, String)](arbHeader)
    body    <- arbitrary[String]
  } yield {
    val res = Response(version, Status(code))
    headers.foreach { case (k, v) => res.headerMap.add(k, v) }
    res.contentString = body
    (res, body)
  }

  val arbRequest = for {
    method  <- arbMethod
    uri     <- arbUri
    version <- Gen.oneOf(Version.Http10, Version.Http11)
    headers <- Gen.containerOf[Seq, (String, String)](arbHeader)
    body    <- arbitrary[String]
  } yield {

    val req = Request.apply(version, method, uri, BufReader(Buf.Utf8(body)))
    headers.foreach { case (k, v) => req.headers.add(k, v) }
    req.setChunked(false)
    req.contentString = body
    req.headers.set(Fields.ContentLength, body.length.toString)
    (req, body)
  }

  val arbNettyMethod =
    Gen.oneOf(
      HttpMethod.GET,
      HttpMethod.POST
    )

  val arbNettyStatus =
    Gen.oneOf(
      HttpResponseStatus.OK,
      HttpResponseStatus.BAD_REQUEST,
      HttpResponseStatus.SERVICE_UNAVAILABLE,
      HttpResponseStatus.GATEWAY_TIMEOUT
    )

  val arbNettyVersion =
    Gen.oneOf(
      HttpVersion.HTTP_1_0,
      HttpVersion.HTTP_1_1,
      new HttpVersion("SECURE-HTTP/1.4", true)
    )

  val arbNettyRequest = for {
    method  <- arbNettyMethod
    uri     <- arbUri
    version <- arbNettyVersion
    kvHeaders <- Gen.containerOf[Seq, (String, String)](arbHeader)
    body    <- arbitrary[String]
  } yield {
    val headers = new DefaultHttpHeaders()
    kvHeaders.foreach { case (k,v) => headers.add(k,v) }
    val req = new DefaultFullHttpRequest(
      version,
      method,
      uri,
      Unpooled.wrappedBuffer(body.getBytes(UTF_8)),
      headers,
      EmptyHttpHeaders.INSTANCE
    )
    (req, body)
  }

  val arbNettyResponse = for {
    method  <- arbNettyMethod
    version <- arbNettyVersion
    status <- arbNettyStatus
    kvHeaders <- Gen.containerOf[Seq, (String, String)](arbHeader)
    body    <- arbitrary[String]
  } yield {
    val headers = new DefaultHttpHeaders
    kvHeaders.foreach { case (k,v) => headers.add(k,v) }
    val req = new DefaultFullHttpResponse(
      version,
      status,
      Unpooled.wrappedBuffer(body.getBytes(UTF_8)),
      headers,
      EmptyHttpHeaders.INSTANCE
    )
    (req, body)
  }
}

@RunWith(classOf[JUnitRunner])
class BijectionsTest extends FunSuite with GeneratorDrivenPropertyChecks {
  import BijectionsTest._


  test("netty http request -> finagle") {
    forAll(arbNettyRequest) { case (in: FullHttpRequest, body: String) =>
      val out = Bijections.netty.requestToFinagle(in)
      assert(out.getUri == in.uri)
      assert(out.isChunked == false)
      assert(out.contentString == body)
      assert(out.version == Bijections.netty.versionToFinagle(in.protocolVersion))
      out.headerMap.foreach { case (k, v) =>
        assert(in.headers.getAll(k).asScala.toSet == out.headerMap.getAll(k).toSet)
      }
    }
  }

  test("netty http response -> finagle") {
    forAll(arbNettyResponse) { case (in: FullHttpResponse, body: String) =>
      val out = Bijections.netty.responseToFinagle(in)
      assert(out.statusCode == in.status.code)
      assert(out.isChunked == false)
      assert(out.contentString == body)
      assert(out.version == Bijections.netty.versionToFinagle(in.protocolVersion))
      out.headerMap.foreach { case (k, v) =>
        assert(in.headers.getAll(k).asScala.toSet == out.headerMap.getAll(k).toSet)
      }
    }
  }

  test("finagle http response -> netty") {
    forAll(arbResponse) { case (in: Response, body: String) =>
      val out = Bijections.finagle.responseToNetty(in)
      assert(HttpUtil.isTransferEncodingChunked(out) == false)
      assert(out.protocolVersion == Bijections.finagle.versionToNetty(in.version))
      assert(out.content.toString(UTF_8) == body)
      in.headerMap.foreach { case (k, v) =>
        assert(out.headers.getAll(k).asScala.toSet == in.headerMap.getAll(k).toSet)
      }
    }
  }

  test("finagle http request -> netty") {
    forAll(arbRequest) { case (in: Request, body: String) =>
      val out = Bijections.finagle.requestToNetty(in)
      assert(HttpUtil.isTransferEncodingChunked(out) == false)
      assert(out.protocolVersion == Bijections.finagle.versionToNetty(in.version))
      assert(out.method == Bijections.finagle.methodToNetty(in.method))
      assert(out.uri == in.getUri)
      assert(out.content.toString(UTF_8) == body)
      in.headerMap.foreach { case (k, v) =>
        assert(out.headers.getAll(k).asScala.toSet == in.headerMap.getAll(k).toSet)
      }
    }
  }
}
