package com.twitter.finagle.netty4.http

import com.twitter.finagle.http._
import com.twitter.io.{Buf, BufReader}
import io.netty.buffer.Unpooled
import io.netty.handler.codec.http.{Cookie => _, _}
import java.net.{InetSocketAddress, URI}
import java.nio.charset.StandardCharsets.UTF_8
import org.scalacheck.{Arbitrary, Gen}
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import scala.jdk.CollectionConverters._
import scala.util.Random
import org.scalatest.funsuite.AnyFunSuite

object BijectionsTest {
  import Arbitrary.arbitrary

  val arbMethod = Gen.oneOf(
    Method.Get,
    Method.Post,
    Method.Trace,
    Method.Delete,
    Method.Put,
    Method.Connect,
    Method.Options
  )

  val arbKeys = Gen.oneOf("Foo", "Bar", "Foo-Bar", "Bar-Baz")

  val arbUri = for {
    scheme <- Gen.oneOf("http", "https")
    hostLen <- Gen.choose(1, 20)
    pathLen <- Gen.choose(1, 20)
    tld <- Gen.oneOf(".net", ".com", "org", ".edu")
    host = Random.alphanumeric.take(hostLen).mkString
    path = Random.alphanumeric.take(pathLen).mkString
  } yield (new URI(scheme, host + tld, "/" + path, null)).toASCIIString

  val arbHeader = for {
    key <- arbKeys
    len <- Gen.choose(0, 10)
  } yield (key, Random.alphanumeric.take(len).mkString)

  val arbResponse = for {
    code <- Gen.chooseNum(100, 510)
    version <- Gen.oneOf(Version.Http10, Version.Http11)
    headers <- Gen.containerOf[Seq, (String, String)](arbHeader)
    body <- arbitrary[String]
  } yield {
    val res = Response(version, Status(code))
    headers.foreach { case (k, v) => res.headerMap.add(k, v) }
    res.contentString = body
    (res, body)
  }

  val arbRequest = for {
    method <- arbMethod
    uri <- arbUri
    version <- Gen.oneOf(Version.Http10, Version.Http11)
    headers <- Gen.containerOf[Seq, (String, String)](arbHeader)
    body <- arbitrary[String]
  } yield {

    val req = Request.apply(version, method, uri, BufReader(Buf.Utf8(body)))
    headers.foreach { case (k, v) => req.headerMap.add(k, v) }
    req.setChunked(false)
    req.contentString = body
    req.contentLength = req.content.length
    (req, body)
  }

  val arbRequestMoreEmptyNotChunked =
    for {
      method <- arbMethod
      uri <- arbUri
      version <- Gen.oneOf(Version.Http10, Version.Http11)
      headers <- Gen.containerOf[Seq, (String, String)](arbHeader)
      body <- Gen.frequency((3, ""), (1, arbitrary[String]))
    } yield {
      val req = Request.apply(version, method, uri, BufReader(Buf.Utf8(body)))
      headers.foreach { case (k, v) => req.headerMap.add(k, v) }
      req.setChunked(false)
      req.contentString = body
      req
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
    method <- arbNettyMethod
    uri <- arbUri
    version <- arbNettyVersion
    kvHeaders <- Gen.containerOf[Seq, (String, String)](arbHeader)
    body <- arbitrary[String]
  } yield {
    val headers = new DefaultHttpHeaders()
    kvHeaders.foreach { case (k, v) => headers.add(k, v) }
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
    version <- arbNettyVersion
    status <- arbNettyStatus
    kvHeaders <- Gen.containerOf[Seq, (String, String)](arbHeader)
    body <- arbitrary[String]
  } yield {
    val headers = new DefaultHttpHeaders
    kvHeaders.foreach { case (k, v) => headers.add(k, v) }
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

class BijectionsTest extends AnyFunSuite with ScalaCheckDrivenPropertyChecks {
  import BijectionsTest._

  test("netty http request -> finagle") {
    forAll(arbNettyRequest) {
      case (in: FullHttpRequest, body: String) =>
        val out = Bijections.netty.fullRequestToFinagle(in, new InetSocketAddress(0))
        assert(out.uri == in.uri)
        assert(out.isChunked == false)
        assert(out.contentString == body)
        assert(out.version == Bijections.netty.versionToFinagle(in.protocolVersion))
        out.headerMap.foreach {
          case (k, v) =>
            assert(in.headers.getAll(k).asScala.toSet == out.headerMap.getAll(k).toSet)
        }
    }
  }

  test("netty http response -> finagle") {
    forAll(arbNettyResponse) {
      case (in: FullHttpResponse, body: String) =>
        val out = Bijections.netty.fullResponseToFinagle(in)
        assert(out.statusCode == in.status.code)
        assert(out.isChunked == false)
        assert(out.contentString == body)
        assert(out.version == Bijections.netty.versionToFinagle(in.protocolVersion))
        out.headerMap.foreach {
          case (k, v) =>
            assert(in.headers.getAll(k).asScala.toSet == out.headerMap.getAll(k).toSet)
        }
    }
  }

  test("finagle http response -> netty") {
    forAll(arbResponse) {
      case (in: Response, body: String) =>
        if (!in.isChunked) {
          val out = Bijections.finagle.fullResponseToNetty(in)
          assert(HttpUtil.isTransferEncodingChunked(out) == false)
          assert(out.protocolVersion == Bijections.finagle.versionToNetty(in.version))
          assert(out.asInstanceOf[FullHttpResponse].content.toString(UTF_8) == body)
          in.headerMap.foreach {
            case (k, v) =>
              assert(out.headers.getAll(k).asScala.toSet == in.headerMap.getAll(k).toSet)
          }
        } else {
          val out = Bijections.finagle.chunkedResponseToNetty(in)
          assert(HttpUtil.isTransferEncodingChunked(out) == false)
          assert(out.protocolVersion == Bijections.finagle.versionToNetty(in.version))
          assert(out.asInstanceOf[FullHttpResponse].content.toString(UTF_8) == body)
          in.headerMap.foreach {
            case (k, v) =>
              assert(out.headers.getAll(k).asScala.toSet == in.headerMap.getAll(k).toSet)
          }
        }
    }
  }

  test("finagle http request -> netty") {
    forAll(arbRequest) {
      case (in: Request, body: String) =>
        val out = Bijections.finagle.requestToNetty(in, in.contentLength)
        assert(HttpUtil.isTransferEncodingChunked(out) == false)
        assert(out.protocolVersion == Bijections.finagle.versionToNetty(in.version))
        assert(out.method == Bijections.finagle.methodToNetty(in.method))
        assert(out.uri == in.uri)
        if (!in.isChunked) {
          assert(out.isInstanceOf[FullHttpRequest])
          val full = out.asInstanceOf[FullHttpRequest]
          assert(full.content.toString(UTF_8) == body)
        }

        in.headerMap.foreach {
          case (k, _) =>
            assert(out.headers.getAll(k).asScala.toSet == in.headerMap.getAll(k).toSet)
        }
    }
  }

  test("incorrect content-length header is fixed on non-chunked requests") {
    val in = Request()
    in.contentString = "foo"
    in.contentLength = 10

    val out = Bijections.finagle.requestToNetty(in, in.contentLength)
    assert(out.headers.get(Fields.ContentLength) == "3")
  }

  test("strips the transfer-encoding header if a content-length header is present") {
    val in = Request()
    in.contentString = "foo"
    in.contentLength = 3
    in.headerMap.set(Fields.TransferEncoding, "chunked")

    val out = Bijections.finagle.requestToNetty(in, in.contentLength)
    assert(out.headers.get(Fields.ContentLength) == "3")
    assert(!HttpUtil.isTransferEncodingChunked(out))
  }

  test("requests(get) that don't have a body don't get an auto-added content-length header") {
    val in = Request(Method.Get, "/")

    val out = Bijections.finagle.requestToNetty(in, in.contentLength)
    assert(!out.headers.contains(Fields.ContentLength))
  }

  test("requests(post) with empty body get an auto-added content-length header") {
    val in = Request(Method.Post, "/")

    val out = Bijections.finagle.requestToNetty(in, in.contentLength)
    assert(out.headers.get(Fields.ContentLength) == "0")
  }

  test("content-length behaviors") {
    forAll(arbRequestMoreEmptyNotChunked) { in =>
      val out = Bijections.finagle.requestToNetty(in, in.contentLength)
      out.method match {
        case HttpMethod.POST | HttpMethod.PATCH | HttpMethod.PUT =>
          assert(out.headers.get(Fields.ContentLength) == in.content.length.toString)
        case _ if in.content.isEmpty =>
          assert(!out.headers.contains(Fields.ContentLength))
        case _ =>
          assert(out.headers.get(Fields.ContentLength) == in.content.length.toString)
      }
    }
  }

  test("finagle http request with chunked and content-length set -> netty") {
    val in = Request()
    in.setChunked(true)
    in.contentLength = 10
    in.headerMap.set(Fields.TransferEncoding, "chunked")

    val out = Bijections.finagle.requestToNetty(in, in.contentLength)
    assert(!HttpUtil.isTransferEncodingChunked(out))
    assert(out.headers.get(Fields.ContentLength) == "10")
  }

  test("finagle http request with chunked and no content-length set -> netty") {
    val in = Request()
    in.setChunked(true)

    val out = Bijections.finagle.requestToNetty(in, in.contentLength)
    assert(HttpUtil.isTransferEncodingChunked(out))
    assert(!out.headers.contains(Fields.ContentLength))
  }

  test("can enable Finagles validation of Netty headers") {
    revalidateInboundHeaders.let(true) {
      val out = HeaderMap()
      val in = new DefaultHttpHeaders( /*validate*/ false)
      in.add("foo:", "bar")
      intercept[IllegalArgumentException](Bijections.netty.writeNettyHeadersToFinagle(in, out))

      in.clear()
      assert(out.isEmpty)
      in.add("foo", "bar\f")
      intercept[IllegalArgumentException](Bijections.netty.writeNettyHeadersToFinagle(in, out))

      in.clear()
      assert(out.isEmpty)
      in.add("foo", "bar\r\n bar")
      Bijections.netty.writeNettyHeadersToFinagle(in, out)
      assert(out("foo") == "bar bar")
    }
  }
}
