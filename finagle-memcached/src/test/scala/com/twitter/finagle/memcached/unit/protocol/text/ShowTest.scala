package com.twitter.finagle.memcached.unit.protocol.text

import com.twitter.finagle.memcached.protocol.text.AbstractCommandToBuf
import com.twitter.finagle.memcached.protocol.text.server.ResponseToBuf
import org.scalacheck.Gen
import org.scalatest.FunSuite
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import com.twitter.finagle.memcached.protocol.{
  ClientError,
  NonexistentCommand,
  ServerError,
  Error => MemcacheError
}
import com.twitter.io.Buf
import com.twitter.io.BufByteWriter
import java.nio.charset.StandardCharsets

class ShowTest extends FunSuite with GeneratorDrivenPropertyChecks {

  test("encode errors - ERROR") {
    val error = MemcacheError(new NonexistentCommand("No such command"))
    val res = ResponseToBuf.encode(error)
    assert(res == Buf.Utf8("ERROR \r\n"))
  }

  test("encode errors - CLIENT_ERROR") {
    val error = MemcacheError(new ClientError("Invalid Input"))
    val res = ResponseToBuf.encode(error)
    assert(res == Buf.Utf8("CLIENT_ERROR Invalid Input \r\n"))
  }

  test("encode errors - SERVER_ERROR") {
    val error = MemcacheError(new ServerError("Out of Memory"))
    val res = ResponseToBuf.encode(error)
    assert(res == Buf.Utf8("SERVER_ERROR Out of Memory \r\n"))
  }

  test("`commandToBuf.lengthAsString` returns same value as `Integer.toString.length`") {
    forAll(Gen.posNum[Int]) { i: Int =>
      assert(AbstractCommandToBuf.lengthAsString(i) == i.toString.length)
    }
  }

  test("`commandToBuf.writeDigits` produces same buffer as `BufByteWriter.writeString`") {
    forAll(Gen.posNum[Int]) { i: Int =>
      val bw1 = BufByteWriter.fixed(16)
      val bw2 = BufByteWriter.fixed(16)
      AbstractCommandToBuf.writeDigits(i, bw1)
      bw2.writeString(i.toString, StandardCharsets.US_ASCII)
      val buf1 = bw1.owned()
      val buf2 = bw2.owned()
      assert(buf1.equals(buf2))
    }
  }
}
