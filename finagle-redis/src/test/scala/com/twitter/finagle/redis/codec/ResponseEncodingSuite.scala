package com.twitter.finagle.redis.naggati

import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.tags.CodecTest
import com.twitter.finagle.redis.util.StringToChannelBuffer
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
final class ResponseEncodingSuite extends RedisResponseTest {

  test("Correctly encode status replies", CodecTest) {
    assert(codec.send(StatusReply("OK")) == List("+OK\r\n"))
  }

  test("Correctly encode error replies", CodecTest) {
    assert(codec.send(ErrorReply("BAD")) == List("-BAD\r\n"))
  }

  test("Correctly encode positive integer replies", CodecTest) {
    assert(codec.send(IntegerReply(123)) == List(":123\r\n"))
  }

  test("Correctly encode negative integer replies", CodecTest) {
    assert(codec.send(IntegerReply(-123)) == List(":-123\r\n"))
  }

  test("Correctly encode bulk replies", CodecTest) {
    assert(codec.send(BulkReply(StringToChannelBuffer("foo\r\nbar"))) ==
      List("$8\r\nfoo\r\nbar\r\n"))
  }

  test("Correctly encode multi bulk replies", CodecTest) {
    val messages = List(BulkReply(StringToChannelBuffer("foo")),
      BulkReply(StringToChannelBuffer("bar")))
    assert(codec.send(MBulkReply(messages)) == List("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"))
  }
}
