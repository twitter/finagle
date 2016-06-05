package com.twitter.finagle.http

import com.twitter.conversions.time._
import com.twitter.io.Buf
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MessageTest extends FunSuite {
  test("empty message") {
    val response = Response()
    assert(response.length == 0)
    assert(response.contentString == "")
  }

  test("headers") {
    val response = Request()
    response.allow.toList == Nil
    response.allow = Method.Get :: Method.Head :: Nil
    assert(response.allow == Some("GET,HEAD"))

    assert(response.accept.toList == Nil)
    response.accept = "text/plain; q=0.5" :: "text/html" :: Nil
    assert(response.accept.toList == "text/plain; q=0.5" :: "text/html" :: Nil)

    response.accept = "A,,c;param,;d,;"
    assert(response.accept.toList == "A" :: "c;param" :: ";d" :: ";" :: Nil)
    assert(response.acceptMediaTypes.toList == "a" :: "c" :: Nil)
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
      assert(request.charset == Option(expected))
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
      assert(request.headers.get("Content-Type") == expected)
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
      assert(request.mediaType == (if (expected.isEmpty) None else Some(expected)))
    }
  }

  test("empty mediaType") {
    val request = Request()
    request.mediaType == None
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
      assert(request.headers.get("Content-Type") == expected)
    }
  }

  test("clearContent") {
    val response = Response()

    response.write("hello")
    response.clearContent()

    assert(response.contentString == "")
    assert(response.length        == 0)
  }

  test("contentString") {
    val response = Response()
    response.content = Buf.Utf8("hello")
    assert(response.contentString == "hello")
    assert(response.contentString == "hello")
  }

  test("cacheControl") {
    val response = Response()

    response.cacheControl = 15123.milliseconds
    assert(response.cacheControl == Some("max-age=15, must-revalidate"))
  }

  test("withInputStream") {
    val response = Response()
    response.content = Buf.Utf8("hello")
    response.withInputStream { inputStream =>
      val bytes = new Array[Byte](5)
      inputStream.read(bytes)
      assert(new String(bytes) == "hello")
    }
  }

  test("withReader") {
    val response = Response()
    response.content = Buf.Utf8("hello")
    response.withReader { reader =>
      val bytes = new Array[Char](5)
      reader.read(bytes)
      assert(new String(bytes) == "hello")
    }
  }

  test("write(String)") {
    val response = Response()
    response.write("hello")
    assert(response.length        == 5)
    assert(response.contentString == "hello")
  }

  test("write(String), multiple writes") {
    val response = Response()
    response.write("h")
    response.write("e")
    response.write("l")
    response.write("l")
    response.write("o")
    assert(response.contentString == "hello")
    assert(response.length        == 5)
  }

  test("withOutputStream") {
    val response = Response()
    response.withOutputStream { outputStream =>
      outputStream.write("hello".getBytes)
    }

    assert(response.contentString == "hello")
    assert(response.length        == 5)
  }

  test("withOutputStream, multiple writes") {
    val response = Response()
    response.write("h")
    response.withOutputStream { outputStream =>
      outputStream.write("e".getBytes)
      outputStream.write("ll".getBytes)
    }
    response.write("o")

    assert(response.contentString == "hello")
    assert(response.length        == 5)
  }

  test("withWriter") {
    val response = Response()
    response.withWriter { writer =>
      writer.write("hello")
    }

    assert(response.contentString == "hello")
    assert(response.length        == 5)
  }

  test("withWriter, multiple writes") {
    val response = Response()
    response.write("h")
    response.withWriter { writer =>
      writer.write("e")
      writer.write("ll")
    }
    response.write("o")

    assert(response.contentString == "hello")
    assert(response.length        == 5)
  }
}
