package com.twitter.finagle.redis.protocol

import com.twitter.finagle.redis.naggati.FinagleRedisRequestTest
import com.twitter.finagle.redis.tags.CodecTest

final class ListCodecSuite extends FinagleRedisRequestTest {

  test("Correctly encode LPUSH for key value pair", CodecTest) {
    val expectedKey   = "foo"
    val expectedValue = "bar"
    unwrap(codec(wrap("LPUSH foo bar\r\n"))) {
      case LPush(key, List(value)) => {
        val actualKey = chanBuf2String(key)
        assert(actualKey === expectedKey)

        val actualValue = chanBuf2String(value)
        assert(actualValue === expectedValue)
      }
    }
  }
}
