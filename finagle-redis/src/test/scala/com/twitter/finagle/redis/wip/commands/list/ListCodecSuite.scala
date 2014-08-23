package com.twitter.finagle.redis.protocol

import com.twitter.conversions.time._
import com.twitter.finagle.redis.ClientError
import com.twitter.finagle.redis.naggati.FinagleRedisRequestTest
import com.twitter.finagle.redis.tags.CodecTest
import com.twitter.finagle.redis.util.CBToString
import com.twitter.util.Time

final class ListCodecSuite extends FinagleRedisRequestTest {

  test("Correctly encode LPUSH for key value pair", CodecTest) {
    val expectedKey = "foo"
    val expectedValue = "bar"
    unwrap(codec(wrap("LPUSH foo bar\r\n"))) {
      case LPush(key, List(value)) => {
        val actualKey = CBToString(key)
        assert(actualKey === expectedKey)

        val actualValue = CBToString(value)
        assert(actualValue === expectedValue)
      }
    }
  }
}
