package com.twitter.finagle.redis.protocol

import com.twitter.finagle.redis.naggati.RedisRequestTest
import com.twitter.finagle.redis.tags.CodecTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
final class ServerCodecSuite extends RedisRequestTest {

  test("Correctly encode FLUSHALL", CodecTest) {
    val actualEncoding = codec(wrap("FLUSHALL\r\n"))
    val expectedEncoding = List(FlushAll)
    assert(actualEncoding === expectedEncoding)
  }
}
