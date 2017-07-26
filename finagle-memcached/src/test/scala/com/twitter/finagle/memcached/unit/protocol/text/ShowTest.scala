package com.twitter.finagle.memcached.unit.protocol.text

import com.twitter.finagle.memcached.protocol.text.server.ResponseToBuf
import org.scalatest.FunSuite
import com.twitter.finagle.memcached.protocol.{
  ClientError,
  NonexistentCommand,
  ServerError,
  Error => MemcacheError
}
import com.twitter.io.Buf

class ShowTest extends FunSuite {

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
}
