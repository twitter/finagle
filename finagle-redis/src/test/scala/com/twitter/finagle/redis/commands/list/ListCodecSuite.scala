package com.twitter.finagle.redis.protocol

import com.twitter.finagle.redis.naggati.RedisRequestTest
import com.twitter.finagle.redis.tags.CodecTest
import com.twitter.finagle.redis.util.BufToString
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
final class ListCodecSuite extends RedisRequestTest {

  test("Correctly encode LPUSH for key value pair", CodecTest) {
    unwrap(codec(wrap("LPUSH foo bar\r\n"))) {
      case LPush(key, List(value)) => {
        assert(BufToString(key) == "foo")
        assert(BufToString(value) == "bar")
      }
    }
  }
}
