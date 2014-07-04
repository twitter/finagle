package com.twitter.finagle.redis.protocol

import com.twitter.finagle.redis.naggati.FinagleRedisRequestTest
import com.twitter.finagle.redis.tags.CodecTest

final class ServerCodecSuite extends FinagleRedisRequestTest {

  test("Correctly encode FLUSHALL", CodecTest) {
    val actualEncoding = codec(wrap("FLUSHALL\r\n"))
    val expectedEncoding = List(FlushAll)
    assert(actualEncoding === expectedEncoding)
  }
}
