package com.twitter.finagle.memcached.unit.protocol.text.server

import com.twitter.finagle.memcached.protocol.{ClientError, Error, NonexistentCommand, ServerError}
import com.twitter.finagle.memcached.protocol.text.server.ResponseToBuf
import com.twitter.io.Buf
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.funsuite.AnyFunSuite

class ResponseToBufTest extends AnyFunSuite with ScalaCheckDrivenPropertyChecks {

  test("encode errors - ERROR") {
    val error = Error(new NonexistentCommand("No such command"))
    val res = ResponseToBuf.encode(error)
    assert(res == Buf.Utf8("ERROR \r\n"))
  }

  test("encode errors - CLIENT_ERROR") {
    val error = Error(new ClientError("Invalid Input"))
    val res = ResponseToBuf.encode(error)
    assert(res == Buf.Utf8("CLIENT_ERROR Invalid Input \r\n"))
  }

  test("encode errors - SERVER_ERROR") {
    val error = Error(new ServerError("Out of Memory"))
    val res = ResponseToBuf.encode(error)
    assert(res == Buf.Utf8("SERVER_ERROR Out of Memory \r\n"))
  }
}
