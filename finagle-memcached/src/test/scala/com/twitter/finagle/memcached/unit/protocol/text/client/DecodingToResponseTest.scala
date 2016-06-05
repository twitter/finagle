package com.twitter.finagle.memcached.unit.protocol.text.client

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import com.twitter.finagle.memcached.protocol
import com.twitter.finagle.memcached.protocol._
import com.twitter.finagle.memcached.protocol.text.client.DecodingToResponse
import com.twitter.finagle.memcached.protocol.text.{StatLines, Tokens}
import com.twitter.io.Buf

@RunWith(classOf[JUnitRunner])
class DecodingToResponseTest extends FunSuite {

  class Context {
    val decodingToResponse = new DecodingToResponse
  }

  test("parseResponse NOT_FOUND") {
    val context = new Context
    import context._

    val buffer = Tokens(Seq(Buf.Utf8("NOT_FOUND")))
    assert(decodingToResponse.decode(null, null, buffer) == NotFound())
  }

  test("parseResponse STORED") {
    val context = new Context
    import context._

    val buffer = Tokens(Seq(Buf.Utf8("STORED")))
    assert(decodingToResponse.decode(null, null, buffer) == Stored())
  }

  test("parseResponse EXISTS") {
    val context = new Context
    import context._

    val buffer = Tokens(Seq(Buf.Utf8("EXISTS")))
    assert(decodingToResponse.decode(null, null, buffer) == Exists())
  }

  test("parseResponse ERROR") {
    val context = new Context
    import context._

    val buffer = Tokens(Seq(Buf.Utf8("ERROR")))
    assert(decodingToResponse
      .decode(null, null, buffer).asInstanceOf[protocol.Error]
      .cause
      .getClass == classOf[NonexistentCommand])
  }

  test("parseResponse STATS") {
    val context = new Context
    import context._

    val lines = Seq(
      Seq("STAT", "items:1:number", "1"),
      Seq("STAT", "items:1:age", "1468"),
      Seq("ITEM", "foo", "[5 b;", "1322514067", "s]"))
    val plines = lines.map { line =>
      Tokens(line map { Buf.Utf8(_) } )
    }
    val info = decodingToResponse.decode(null, null, StatLines(plines))
    assert(info.getClass == classOf[InfoLines])
    val ilines = info.asInstanceOf[InfoLines].lines
    assert(ilines.size == lines.size)
    ilines.zipWithIndex.foreach { case(line, idx) =>
      val key = lines(idx)(0)
      val values = lines(idx).drop(1)
      assert(line.key == Buf.Utf8(key))
      assert(line.values.size == values.size)
      line.values.zipWithIndex.foreach { case(token, tokIdx) =>
        assert(token == Buf.Utf8(values(tokIdx)))
      }
    }
  }

  test("parseResponse CLIENT_ERROR") {
    val context = new Context
    import context._

    val errorMessage = "sad panda error"
    val buffer = Tokens(Seq("CLIENT_ERROR", errorMessage).map(Buf.Utf8(_)))
    val error = decodingToResponse.decode(null, null, buffer).asInstanceOf[protocol.Error]
    assert(error.cause.getClass == classOf[ClientError])
    assert(error.cause.getMessage() == errorMessage)
  }

  test("parseResponse SERVER_ERROR") {
    val context = new Context
    import context._

    val errorMessage = "sad panda error"
    val buffer = Tokens(Seq("SERVER_ERROR", "sad", "panda", "error").map(Buf.Utf8(_)))
    val error = decodingToResponse.decode(null, null, buffer).asInstanceOf[protocol.Error]
    assert(error.cause.getClass == classOf[ServerError])
    assert(error.cause.getMessage() == errorMessage)
  }

}
