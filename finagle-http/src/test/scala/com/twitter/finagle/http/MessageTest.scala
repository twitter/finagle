package com.twitter.finagle.http

import com.twitter.conversions.time._
import org.jboss.netty.buffer.ChannelBuffers
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MessageTest extends FunSuite {
  test("empty message") {
    val response = Response()
    assert(response.length === 0)
    assert(response.contentString === "")
  }

  test("headers") {
    val response = Request()
    response.allow.toList === Nil
    response.allow = Method.Get :: Method.Head :: Nil
    assert(response.allow === Some("GET,HEAD"))

    assert(response.accept.toList === Nil)
    response.accept = "text/plain; q=0.5" :: "text/html" :: Nil
    assert(response.accept.toList === "text/plain; q=0.5" :: "text/html" :: Nil)

    response.accept = "A,,c;param,;d,;"
    assert(response.accept.toList === "A" :: "c;param" :: ";d" :: ";" :: Nil)
    assert(response.acceptMediaTypes.toList === "a" :: "c" :: Nil)
  }

  test("ascii encoding protects against header injection") {
    // A request with a "Foo" key, containing the injected header "Bar".
    val req = Request("/?Foo=%E5%98%8D%E5%98%8ABar:%20injection")
    val (fooKey, fooVal) = req.params.toList.head
    // We encode the response to a string containing raw HTTP. HTTP is
    // delimited by CRLF, which we exploit to crudely (but effectively) extract
    // HTTP header lines. Because we set only one header for "Foo" we should
    // not also see a header "Bar".
    val res = Response()
    res.headers.set(fooKey, fooVal)
    val lines = res.encodeString.split("\r\n")
    assert(lines.exists(_.startsWith("Foo:")))
    assert(!lines.exists(_.startsWith("Bar:")))
  }

  test("charset") {
    val tests = Map(
      "x; charset=a" -> "a",
      "x;charset=a" -> "a",
      "x;  charset=a  " -> "a",
      "x;y;charset=a" ->"a",
      "x; charset="  -> "",
      "x; charset==" -> "=",
      "x; charset" -> null,
      "x" -> null,
      ";;;;;;" -> null
    )
    tests.foreach { case (header, expected) =>
      val request = Request()
      request.headers.set("Content-Type", header)
      assert(request.charset === Option(expected))
    }
  }

  test("charset=") {
    val tests = Map(
      ("x; charset=a" -> "b") -> "x;charset=b",
      ("x" -> "b") -> "x;charset=b",
      ("x;p1" -> "b") -> "x;charset=b;p1",
      ("x;p1; p2 ;p3" -> "b") -> "x;charset=b;p1; p2 ;p3",
      ("x;p1;charset=a;p3" -> "b") -> "x;p1;charset=b;p3",
      ("x;" -> "b") -> "x;charset=b",
      (";"  -> "b") -> ";charset=b",
      ("" -> "b") -> ";charset=b"
    )
    tests.foreach { case ((header, charset), expected) =>
      val request = Request()
      request.headers.set("Content-Type", header)
      request.charset = charset
      assert(request.headers.get("Content-Type") === expected)
    }
  }

  test("mediaType") {
    val tests = Map(
      "application/json" -> "application/json",
      "application/json;charset=utf-8" -> "application/json",
      "" -> "",
      ";" -> "",
      ";;;;;;;;;;" -> "",
      "application/json;" -> "application/json",
      "  application/json  ;  charset=utf-8  " -> "application/json",
      "APPLICATION/JSON" -> "application/json"
    )
    tests.foreach { case (header, expected) =>
      val request = Request()
      request.headers.set("Content-Type", header)
      // shorthand for empty mediaTypes really being returned as None after being parsed.
      assert(request.mediaType === (if (expected.isEmpty) None else Some(expected)))
    }
  }

  test("empty mediaType") {
    val request = Request()
    request.mediaType === None
  }

  test("mediaType=") {
    val tests = Map(
      ("x" -> "y") -> "y",
      ("x; charset=a" -> "y") -> "y; charset=a",
      ("x;p1; p2 ;p3" -> "y") -> "y;p1; p2 ;p3",
      ("x;" -> "y") -> "y",
      (";" -> "y") -> "y",
      ("" -> "y") -> "y"
    )
    tests.foreach { case ((header, mediaType), expected) =>
      val request = Request()
      request.headers.set("Content-Type", header)
      request.mediaType = mediaType
      assert(request.headers.get("Content-Type") === expected)
    }
  }

  test("clearContent") {
    val response = Response()

    response.write("hello")
    response.clearContent()

    assert(response.contentString === "")
    assert(response.length        === 0)
  }

  test("contentString") {
    val response = Response()
    response.setContent(ChannelBuffers.wrappedBuffer("hello".getBytes))
    assert(response.contentString === "hello")
    assert(response.contentString === "hello")
  }

  test("cacheControl") {
    val response = Response()

    response.cacheControl = 15123.milliseconds
    assert(response.cacheControl === Some("max-age=15, must-revalidate"))
  }

  test("withInputStream") {
    val response = Response()
    response.setContent(ChannelBuffers.wrappedBuffer("hello".getBytes))
    response.withInputStream { inputStream =>
      val bytes = new Array[Byte](5)
      inputStream.read(bytes)
      assert(new String(bytes) === "hello")
    }
  }

  test("withReader") {
    val response = Response()
    response.setContent(ChannelBuffers.wrappedBuffer("hello".getBytes))
    response.withReader { reader =>
      val bytes = new Array[Char](5)
      reader.read(bytes)
      assert(new String(bytes) === "hello")
    }
  }

  test("write(String)") {
    val response = Response()
    response.write("hello")
    assert(response.length        === 5)
    assert(response.contentString === "hello")
  }

  test("write(String), multiple writes") {
    val response = Response()
    response.write("h")
    response.write("e")
    response.write("l")
    response.write("l")
    response.write("o")
    assert(response.contentString === "hello")
    assert(response.length        === 5)
  }

  test("withOutputStream") {
    val response = Response()
    response.withOutputStream { outputStream =>
      outputStream.write("hello".getBytes)
    }

    assert(response.contentString === "hello")
    assert(response.length        === 5)
  }

  test("withOutputStream, multiple writes") {
    val response = Response()
    response.write("h")
    response.withOutputStream { outputStream =>
      outputStream.write("e".getBytes)
      outputStream.write("ll".getBytes)
    }
    response.write("o")

    assert(response.contentString === "hello")
    assert(response.length        === 5)
  }

  test("withWriter") {
    val response = Response()
    response.withWriter { writer =>
      writer.write("hello")
    }

    assert(response.contentString === "hello")
    assert(response.length        === 5)
  }

  test("withWriter, multiple writes") {
    val response = Response()
    response.write("h")
    response.withWriter { writer =>
      writer.write("e")
      writer.write("ll")
    }
    response.write("o")

    assert(response.contentString === "hello")
    assert(response.length        === 5)
  }
}
