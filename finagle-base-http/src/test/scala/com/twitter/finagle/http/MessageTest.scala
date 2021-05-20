package com.twitter.finagle.http

import com.twitter.conversions.DurationOps._
import com.twitter.io.Buf
import java.time.ZonedDateTime
import java.util.Date
import org.scalatest.funsuite.AnyFunSuite

class MessageTest extends AnyFunSuite {

  private def defaultMessages(): Seq[Message] = Seq(Request(), Response())

  test("empty message") {
    val response = Response()
    assert(response.length == 0)
    assert(response.contentString == "")
  }

  test("version") {
    Seq(
      Request(Version.Http10, Method.Get, ""),
      Response(Version.Http10, Status.Ok)
    ).foreach { message =>
      assert(message.version == Version.Http10)

      message.version = Version.Http11
      assert(message.version == Version.Http11)

      message.version(Version.Http10)
      assert(message.version == Version.Http10)
    }
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

    assert(response.date == None)
    response.date = new Date(0L)
    assert(response.date == Some("Thu, 01 Jan 1970 00:00:00 GMT"))
  }

  test("charset") {
    val tests = Map(
      "x; charset=a" -> "a",
      "x;charset=a" -> "a",
      "x;  charset=a  " -> "a",
      "x;y;charset=a" -> "a",
      "x; charset=" -> "",
      "x; charset==" -> "=",
      "x; charset" -> null,
      "x" -> null,
      ";;;;;;" -> null
    )
    tests.foreach {
      case (header, expected) =>
        withClue(header) {
          val request = Request()
          request.headerMap.set("Content-Type", header)
          assert(request.charset == Option(expected))
        }
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
      (";" -> "b") -> ";charset=b",
      ("" -> "b") -> ";charset=b"
    )
    tests.foreach {
      case ((header, charset), expected) =>
        val request = Request()
        request.headerMap.set("Content-Type", header)
        request.charset = charset
        assert(request.headerMap("Content-Type") == expected)
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
    tests.foreach {
      case (header, expected) =>
        val request = Request()
        request.headerMap.set("Content-Type", header)
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
    tests.foreach {
      case ((header, mediaType), expected) =>
        val request = Request()
        request.headerMap.set("Content-Type", header)
        request.mediaType = mediaType
        assert(request.headerMap("Content-Type") == expected)
    }
  }

  test("clearContent") {
    val response = Response()

    response.write("hello")
    response.clearContent()

    assert(response.contentString == "")
    assert(response.length == 0)
  }

  test("set content") {
    val buf = Buf.ByteArray(0, 1, 2, 3)
    defaultMessages().foreach { msg =>
      assert(msg.content.isEmpty)
      msg.content = buf
      assert(msg.content == buf)
    }
  }

  test("content(Buf)") {
    val buf = Buf.ByteArray(0, 1, 2, 3)
    defaultMessages().foreach { msg =>
      assert(msg.content.isEmpty)
      msg.content(buf)
      assert(msg.content == buf)
    }
  }

  test("set non-empty content when chunked throws IllegalStateException") {
    defaultMessages().foreach { msg =>
      msg.setChunked(true)
      assert(msg.content.isEmpty)

      msg.content(Buf.Empty) // It's fine to set 0 length content

      intercept[IllegalStateException] {
        msg.content = Buf.ByteArray(1)
      }

      intercept[IllegalStateException] {
        msg.content(Buf.ByteArray(1))
      }
    }
  }

  test("setting message to chunked will remove message content") {
    defaultMessages().foreach { msg =>
      msg.content = Buf.ByteArray(1, 2, 3, 4)
      msg.setChunked(true)
      assert(msg.content.isEmpty)
    }
  }

  test("setting message from chunked to not chunked will allow manipulation of the content") {
    val buf = Buf.ByteArray(1, 2, 3, 4)
    defaultMessages().foreach { msg =>
      msg.setChunked(true)
      intercept[IllegalStateException] {
        msg.content(Buf.ByteArray(1))
      }

      msg.setChunked(false)
      msg.content = buf // Now legal
      assert(buf == msg.content)
    }
  }

  test("the `content` of a chunked Message is always empty") {
    val buf = Buf.ByteArray(1, 2, 3, 4)
    defaultMessages().foreach { msg =>
      msg.content = buf
      msg.setChunked(true)
      assert(msg.content.isEmpty)
      msg.writer.write(buf) // don't care to wait
      assert(msg.content.isEmpty)
    }
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
    assert(response.length == 5)
    assert(response.contentString == "hello")
  }

  test("write(..) on chunked message throws and IllegalStateException") {
    defaultMessages().foreach { msg =>
      msg.setChunked(true)

      intercept[IllegalStateException] {
        msg.write("illegal")
      }

      intercept[IllegalStateException] {
        msg.write(Array[Byte](0, 1, 2, 3))
      }

      intercept[IllegalStateException] {
        msg.write(Buf.ByteArray(0, 1, 2, 3))
      }
    }
  }

  test("write(String), multiple writes") {
    val response = Response()
    response.write("h")
    response.write("e")
    response.write("l")
    response.write("l")
    response.write("o")
    assert(response.contentString == "hello")
    assert(response.length == 5)
  }

  test("withOutputStream") {
    val response = Response()
    response.withOutputStream { outputStream => outputStream.write("hello".getBytes) }

    assert(response.contentString == "hello")
    assert(response.length == 5)
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
    assert(response.length == 5)
  }

  test("withWriter") {
    val response = Response()
    response.withWriter { writer => writer.write("hello") }

    assert(response.contentString == "hello")
    assert(response.length == 5)
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
    assert(response.length == 5)
  }

  test("httpDateFormat") {
    assert(Message.httpDateFormat(new Date(0L)) == "Thu, 01 Jan 1970 00:00:00 GMT")
    assert(Message.httpDateFormat(0L) == "Thu, 01 Jan 1970 00:00:00 GMT")

    val timeGMT: Date = Date.from(ZonedDateTime.parse("2012-06-30T12:30:40Z[GMT]").toInstant)
    val timeUTC: Date = Date.from(ZonedDateTime.parse("2012-06-30T12:30:40Z[UTC]").toInstant)
    val timeLASummer: Date =
      Date.from(ZonedDateTime.parse("2012-06-30T12:30:40-07:00[America/Los_Angeles]").toInstant)
    val timeLAWinter: Date =
      Date.from(ZonedDateTime.parse("2012-12-30T12:30:40-07:00[America/Los_Angeles]").toInstant)
    val timeSH: Date =
      Date.from(ZonedDateTime.parse("2012-06-03T12:30:40+08:00[Asia/Shanghai]").toInstant)
    val timeEurope: Date =
      Date.from(ZonedDateTime.parse("2012-06-30T12:30:40+01:00[Europe/London]").toInstant)

    assert(Message.httpDateFormat(timeGMT) == "Sat, 30 Jun 2012 12:30:40 GMT")
    assert(
      Message.httpDateFormat(timeGMT.toInstant.toEpochMilli) == "Sat, 30 Jun 2012 12:30:40 GMT"
    )
    assert(Message.httpDateFormat(timeUTC) == "Sat, 30 Jun 2012 12:30:40 GMT")
    assert(
      Message.httpDateFormat(timeUTC.toInstant.toEpochMilli) == "Sat, 30 Jun 2012 12:30:40 GMT"
    )
    assert(Message.httpDateFormat(timeLASummer) == "Sat, 30 Jun 2012 19:30:40 GMT")
    assert(
      Message.httpDateFormat(timeLASummer.toInstant.toEpochMilli) == "Sat, 30 Jun 2012 19:30:40 GMT"
    )

    // workaround for https://bugs.openjdk.java.net/browse/JDK-8066982
    if (sys.props("java.version").startsWith("1.")) {
      assert(Message.httpDateFormat(timeLAWinter) == "Sun, 30 Dec 2012 20:30:40 GMT")
      assert(
        Message
          .httpDateFormat(timeLAWinter.toInstant.toEpochMilli) == "Sun, 30 Dec 2012 20:30:40 GMT"
      )
    } else {
      assert(Message.httpDateFormat(timeLAWinter) == "Sun, 30 Dec 2012 19:30:40 GMT")
      assert(
        Message
          .httpDateFormat(timeLAWinter.toInstant.toEpochMilli) == "Sun, 30 Dec 2012 19:30:40 GMT"
      )
    }
    assert(Message.httpDateFormat(timeSH) == "Sun, 03 Jun 2012 04:30:40 GMT")
    assert(Message.httpDateFormat(timeSH.toInstant.toEpochMilli) == "Sun, 03 Jun 2012 04:30:40 GMT")
    assert(Message.httpDateFormat(timeEurope) == "Sat, 30 Jun 2012 11:30:40 GMT")
    assert(
      Message.httpDateFormat(timeEurope.toInstant.toEpochMilli) == "Sat, 30 Jun 2012 11:30:40 GMT"
    )
  }
}
