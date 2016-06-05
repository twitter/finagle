package com.twitter.finagle.http.filter

import com.twitter.finagle.Service
import com.twitter.finagle.http.{Method, Request, Response, Version}
import com.twitter.logging.{BareFormatter, Logger, StringHandler}
import com.twitter.util.{Await, Future, Time}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class LoggingFilterTest extends FunSuite {

  test("log") {
    val logger = Logger.get("access")
    logger.setLevel(Logger.INFO)
    val stringHandler = new StringHandler(BareFormatter, Some(Logger.INFO))
    logger.addHandler(stringHandler)
    logger.setUseParentHandlers(false)

    val request = Request("/search.json")
    request.method = Method.Get
    request.xForwardedFor = "10.0.0.1"
    request.referer       = "http://www.example.com/"
    request.userAgent     = "User Agent"
    request.version = Version.Http11

    val formatter = new CommonLogFormatter
    val service = new Service[Request, Response] {
      def apply(request: Request): Future[Response] = {
        val response = request.response
        response.statusCode = 123
        response.write("hello")
        Future.value(response)
      }
    }
    val filter = (new LoggingFilter(logger, formatter)) andThen service

    Time.withTimeAt(Time.fromSeconds(1302121932)) { _ =>
      Await.result(filter(request))
    }

    stringHandler.get == ("""127\.0\.0\.1 - - \[06/Apr/2011:20:32:12 \+0000\] "GET /search\.json HTTP/1\.1" 123 5 [0-9]+ "User Agent"""" + "\n")
  }

  val UnescapedEscaped =
    Seq(
      // boundaries
      ("",        ""),
      ("hello\n", "hello\\n"),
      ("\nhello", "\\nhello"),
      // low ascii and special characters
      ("\u0000", "\\x00"),
      ("\u0001", "\\x01"),
      ("\u0002", "\\x02"),
      ("\u0003", "\\x03"),
      ("\u0004", "\\x04"),
      ("\u0005", "\\x05"),
      ("\u0006", "\\x06"),
      ("\u0007", "\\x07"),
      ("\u0008", "\\b"),
      ("\u0009", "\\t"),
      ("\u000a", "\\n"),
      ("\u000b", "\\v"),
      ("\u000c", "\\x0c"),
      ("\u000d", "\\r"),
      ("\u000e", "\\x0e"),
      ("\u000f", "\\x0f"),
      ("\u0010", "\\x10"),
      ("\u0011", "\\x11"),
      ("\u0012", "\\x12"),
      ("\u0013", "\\x13"),
      ("\u0014", "\\x14"),
      ("\u0015", "\\x15"),
      ("\u0016", "\\x16"),
      ("\u0017", "\\x17"),
      ("\u0018", "\\x18"),
      ("\u0019", "\\x19"),
      ("\u001a", "\\x1a"),
      ("\u001b", "\\x1b"),
      ("\u001c", "\\x1c"),
      ("\u001d", "\\x1d"),
      ("\u001e", "\\x1e"),
      ("\u001f", "\\x1f"),
      ("\u0020", " "),
      ("\u0021", "!"),
      ("\"",     "\\\""),
      ("\u0023", "#"),
      ("\u0024", "$"),
      ("\u0025", "%"),
      ("\u0026", "&"),
      ("\u0027", "'"),
      ("\u0028", "("),
      ("\u0029", ")"),
      ("\u002a", "*"),
      ("\u002b", "+"),
      ("\u002c", ","),
      ("\u002d", "-"),
      ("\u002e", "."),
      ("\u002f", "/"),
      ("\u0030", "0"),
      ("\u0031", "1"),
      ("\u0032", "2"),
      ("\u0033", "3"),
      ("\u0034", "4"),
      ("\u0035", "5"),
      ("\u0036", "6"),
      ("\u0037", "7"),
      ("\u0038", "8"),
      ("\u0039", "9"),
      ("\u003a", ":"),
      ("\u003b", ";"),
      ("\u003c", "<"),
      ("\u003d", "="),
      ("\u003e", ">"),
      ("\u003f", "?"),
      ("\u0040", "@"),
      ("\u0041", "A"),
      ("\u0042", "B"),
      ("\u0043", "C"),
      ("\u0044", "D"),
      ("\u0045", "E"),
      ("\u0046", "F"),
      ("\u0047", "G"),
      ("\u0048", "H"),
      ("\u0049", "I"),
      ("\u004a", "J"),
      ("\u004b", "K"),
      ("\u004c", "L"),
      ("\u004d", "M"),
      ("\u004e", "N"),
      ("\u004f", "O"),
      ("\u0050", "P"),
      ("\u0051", "Q"),
      ("\u0052", "R"),
      ("\u0053", "S"),
      ("\u0054", "T"),
      ("\u0055", "U"),
      ("\u0056", "V"),
      ("\u0057", "W"),
      ("\u0058", "X"),
      ("\u0059", "Y"),
      ("\u005a", "Z"),
      ("\u005b", "["),
      ("\\",     "\\\\"),
      ("\u005d", "]"),
      ("\u005e", "^"),
      ("\u005f", "_"),
      ("\u0060", "`"),
      ("\u0061", "a"),
      ("\u0062", "b"),
      ("\u0063", "c"),
      ("\u0064", "d"),
      ("\u0065", "e"),
      ("\u0066", "f"),
      ("\u0067", "g"),
      ("\u0068", "h"),
      ("\u0069", "i"),
      ("\u006a", "j"),
      ("\u006b", "k"),
      ("\u006c", "l"),
      ("\u006d", "m"),
      ("\u006e", "n"),
      ("\u006f", "o"),
      ("\u0070", "p"),
      ("\u0071", "q"),
      ("\u0072", "r"),
      ("\u0073", "s"),
      ("\u0074", "t"),
      ("\u0075", "u"),
      ("\u0076", "v"),
      ("\u0077", "w"),
      ("\u0078", "x"),
      ("\u0079", "y"),
      ("\u007a", "z"),
      ("\u007b", "{"),
      ("\u007c", "|"),
      ("\u007d", "}"),
      ("\u007e", "~"),
      ("\u007f", "\\x7f"),
      ("\u0080", "\\xc2\\x80"),
      ("\u00e9", "\\xc3\\xa9"), // é
      ("\u2603", "\\xe2\\x98\\x83") // snowman
    )

  test("escape() escapes non-printable, non-ASCII") {
    UnescapedEscaped.foreach { case (input, escaped) =>
      assert(LogFormatter.escape(input) == escaped)
    }
  }
}
