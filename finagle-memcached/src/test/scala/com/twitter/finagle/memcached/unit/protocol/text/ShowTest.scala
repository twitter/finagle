package com.twitter.finagle.memcached.protocol.text

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import com.twitter.finagle.memcached.protocol.{
  Error => MemcacheError, ClientError, NonexistentCommand, ServerError}
import com.twitter.io.Buf

@RunWith(classOf[JUnitRunner])
class ShowTest extends FunSuite {
  val responseToEncoding = new ResponseToEncoding

  test("encode errors - ERROR") {
    val error = MemcacheError(new NonexistentCommand("No such command"))
    val res = responseToEncoding.encode(null, null, error)
    assert(res.getClass == classOf[Tokens])
    val tokens = res.asInstanceOf[Tokens]
    assert(tokens.tokens.size == 1)
    val Buf.Utf8(result) = tokens.tokens.head
    assert(result == "ERROR")
  }

  test("encode errors - CLIENT_ERROR") {
    val error = MemcacheError(new ClientError("Invalid Input"))
    val res = responseToEncoding.encode(null, null, error)
    assert(res.getClass == classOf[Tokens])
    val tokens = res.asInstanceOf[Tokens]
    assert(tokens.tokens.size == 2)
    val Buf.Utf8(result) = tokens.tokens.head
    assert(result == "CLIENT_ERROR")
  }

  test("encode errors - SERVER_ERROR") {
    val error = MemcacheError(new ServerError("Out of Memory"))
    val res = responseToEncoding.encode(null, null, error)
    assert(res.getClass == classOf[Tokens])
    val tokens = res.asInstanceOf[Tokens]
    assert(tokens.tokens.size == 2)

    val Buf.Utf8(result) = tokens.tokens.head
    assert(result == "SERVER_ERROR")
  }
}
