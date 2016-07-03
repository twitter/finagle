package com.twitter.finagle.redis.protocol

import com.twitter.finagle.redis.naggati.RedisRequestTest
import com.twitter.finagle.redis.tags.CodecTest
import com.twitter.io.Buf
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
final class RequestEncodingSuite extends RedisRequestTest {

  test("Correctly encode inline requests", CodecTest) {
    assert(codec.send(Get(bufFoo)) == List("*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n"))
  }

  test("Correctly encode unified requests", CodecTest) {
    val value = "bar\r\nbaz"
    assert(codec.send(Set(bufFoo, Buf.Utf8(value))) ==
      List("*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$%d\r\n%s\r\n".format(8, value)))
  }

  test("Correctly encode a HSet request with an empty string as value", CodecTest) {
    assert(codec.send(HSet(bufFoo, bufBar, Buf.Empty)) ==
      List("*4\r\n$4\r\nHSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$0\r\n\r\n"))
  }
}
