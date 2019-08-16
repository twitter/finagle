package com.twitter.finagle.redis.replies

import com.twitter.finagle.redis.{RedisResponseTest, ServerError}
import com.twitter.finagle.redis.protocol._
import com.twitter.io.Buf

class ReplyDecodingTest extends RedisResponseTest {

  test("string") {
    forAll(genChunkedReply[StatusReply])(testDecodingInChunks)
  }

  test("string (empty)") {
    intercept[ServerError] { decodeReply("+\r\n") }
  }

  test("error") {
    forAll(genChunkedReply[ErrorReply])(testDecodingInChunks)
  }

  test("error (empty)") {
    intercept[ServerError] { decodeReply("-\r\n") }
  }

  test("integer") {
    forAll(genChunkedReply[IntegerReply])(testDecodingInChunks)
  }

  test("integer (too small)") {
    intercept[ServerError] { decodeReply(s":-9223372036854775809\r\n") }
  }

  test("integer (too big)") {
    intercept[ServerError] { decodeReply(s":9223372036854775808\r\n") }
  }

  test("bulk") {
    forAll(genChunkedReply[BulkReply])(testDecodingInChunks)
  }

  test("bulk (empty)") {
    assert(decodeReply("$0\r\n\r\n").contains(BulkReply(Buf.Empty)))
  }

  test("bulk (nil)") {
    assert(decodeReply("$-1\r\n").contains(EmptyBulkReply))
  }

  test("array") {
    forAll(genChunkedReply[MBulkReply])(testDecodingInChunks)
  }

  test("array (empty)") {
    assert(decodeReply("*0\r\n").contains(EmptyMBulkReply))
  }

  test("array (nil)") {
    assert(decodeReply("*-1\r\n").contains(NilMBulkReply))
  }
}
