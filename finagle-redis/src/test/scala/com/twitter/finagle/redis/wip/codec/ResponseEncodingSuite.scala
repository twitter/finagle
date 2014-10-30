package com.twitter.finagle.redis.naggati

import com.twitter.finagle.redis.protocol.{BulkReply, ErrorReply, IntegerReply, MBulkReply,
                                          StatusReply, ReplyCodec}
import com.twitter.finagle.redis.tags.CodecTest
import com.twitter.finagle.redis.util.StringToChannelBuffer
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner


@RunWith(classOf[JUnitRunner])
final class ResponseEncodingSuite extends RedisResponseTest {

  test("Correctly encode status replies", CodecTest) {
    val actualEncoding = codec.send(StatusReply("OK"))
    val expectedEncoding = List("+OK\r\n")
    assert(actualEncoding === expectedEncoding)
  }

  test("Correctly encode error replies", CodecTest) {
    val actualEncoding = codec.send(ErrorReply("BAD"))
    val expectedEncoding = List("-BAD\r\n")
    assert(actualEncoding === expectedEncoding)
  }

  test("Correctly encode positive integer replies", CodecTest) {
    val actualEncoding = codec.send(IntegerReply(123))
    val expectedEncoding = List(":123\r\n")
    assert(actualEncoding === expectedEncoding)
  }

  test("Correctly encode negative integer replies", CodecTest) {
    val actualEncoding = codec.send(IntegerReply(-123))
    val expectedEncoding = List(":-123\r\n")
    assert(actualEncoding === expectedEncoding)
  }

  test("Correctly encode bulk replies", CodecTest) {
    val actualEncoding = codec.send(BulkReply(StringToChannelBuffer("foo\r\nbar")))
    val expectedEncoding = List("$8\r\nfoo\r\nbar\r\n")
    assert(actualEncoding === expectedEncoding)
  }

  test("Correctly encode multi bulk replies", CodecTest) {
    val messages = List(BulkReply(StringToChannelBuffer("foo")),
      BulkReply(StringToChannelBuffer("bar")))
    val actualEncoding = codec.send(MBulkReply(messages))
    val expectedEncoding = List("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
    assert(actualEncoding === expectedEncoding)
  }
}
