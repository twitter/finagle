package com.twitter.finagle.memcached.protocol.text

import com.twitter.finagle.memcached.protocol.{
  Error => MemcacheError, ClientError, NonexistentCommand, ServerError}
import com.twitter.io.Charsets
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ShowTest extends FunSuite {
  val responseToEncoding = new ResponseToEncoding

  test("encode errors - ERROR") {
    val error = MemcacheError(new NonexistentCommand("No such command"))
    val res = responseToEncoding.encode(null, null, error)
    assert(res.getClass === classOf[Tokens])
    val tokens = res.asInstanceOf[Tokens]
    assert(tokens.tokens.size === 1)
    assert(tokens.tokens.head.toString(Charsets.Utf8) === "ERROR")
  }

  test("encode errors - CLIENT_ERROR") {
    val error = MemcacheError(new ClientError("Invalid Input"))
    val res = responseToEncoding.encode(null, null, error)
    assert(res.getClass === classOf[Tokens])
    val tokens = res.asInstanceOf[Tokens]
    assert(tokens.tokens.size === 2)
    assert(tokens.tokens.head.toString(Charsets.Utf8) === "CLIENT_ERROR")
  }

  test("encode errors - SERVER_ERROR") {
    val error = MemcacheError(new ServerError("Out of Memory"))
    val res = responseToEncoding.encode(null, null, error)
    assert(res.getClass === classOf[Tokens])
    val tokens = res.asInstanceOf[Tokens]
    assert(tokens.tokens.size === 2)
    assert(tokens.tokens.head.toString(Charsets.Utf8) === "SERVER_ERROR")
  }
}
