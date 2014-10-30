package com.twitter.finagle.redis.protocol

import com.twitter.finagle.redis.naggati.RedisRequestTest
import com.twitter.finagle.redis.tags.CodecTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
final class RequestEncodingSuite extends RedisRequestTest {

  test("Correctly encode inline requests", CodecTest) {
    val actualEncoding = codec.send(Get(foo))
    val expectedEncoding = List("*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n")
    assert(actualEncoding === expectedEncoding)
  }

  test("Correctly encode unified requests", CodecTest) {
    val value = "bar\r\nbaz"
    val actualEncoding = codec.send(Set(foo, string2ChanBuf(value)))
    val expectedEncoding = List("*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$%d\r\n%s\r\n".format(8, value))
    assert(actualEncoding === expectedEncoding)
  }

  test("Correctly encode a HSet request with an empty string as value", CodecTest) {
    val actualEncoding = codec.send(HSet(foo, bar, string2ChanBuf("")))
    val expectedEncoding = List("*4\r\n$4\r\nHSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$0\r\n\r\n")
    assert(actualEncoding === expectedEncoding)
  }
}
