package com.twitter.finagle.memcached.unit.protocol.text.server

import com.twitter.finagle.memcached.protocol.ClientError
import com.twitter.finagle.memcached.protocol.Error
import com.twitter.finagle.memcached.protocol.NonexistentCommand
import com.twitter.finagle.memcached.protocol.ServerError
import com.twitter.finagle.memcached.protocol.text.server.ResponseToBuf
import com.twitter.io.Buf
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.funsuite.AnyFunSuite

class ResponseToBufTest extends AnyFunSuite with ScalaCheckDrivenPropertyChecks {

  test("ERROR") {
    val error = Error(new NonexistentCommand("No such command"))
    val res = ResponseToBuf.encode(error)
    assert(res == Buf.Utf8("ERROR\r\n"))
  }

  test("CLIENT_ERROR") {
    val errorNoTrailingWhitespace = Error(new ClientError("Invalid Input"))
    assert(
      ResponseToBuf.encode(errorNoTrailingWhitespace) == Buf.Utf8("CLIENT_ERROR Invalid Input\r\n"))

    val errorTrailingWhitespace = Error(new ClientError("Invalid Input "))
    assert(
      ResponseToBuf.encode(errorTrailingWhitespace) == Buf.Utf8("CLIENT_ERROR Invalid Input \r\n"))
  }

  test("SERVER_ERROR") {
    val errorNoTrailingWhitespace = Error(new ServerError("Out of Memory"))
    assert(
      ResponseToBuf.encode(errorNoTrailingWhitespace) == Buf.Utf8("SERVER_ERROR Out of Memory\r\n"))

    val errorTrailingWhitespace = Error(new ServerError("Out of Memory "))
    assert(
      ResponseToBuf.encode(errorTrailingWhitespace) == Buf.Utf8("SERVER_ERROR Out of Memory \r\n"))
  }
}
