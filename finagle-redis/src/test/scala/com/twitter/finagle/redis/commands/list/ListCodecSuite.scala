package com.twitter.finagle.redis.protocol

import com.twitter.finagle.redis.naggati.RedisRequestTest
import com.twitter.finagle.redis.tags.CodecTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
final class ListCodecSuite extends RedisRequestTest {

  test("Correctly encode LPUSH for key value pair", CodecTest) {
    unwrap(codec(wrap("LPUSH foo bar\r\n"))) {
      case LPush(key, List(value)) => {
        assert(chanBuf2String(key) == "foo")
        assert(chanBuf2String(value) == "bar")
      }
    }
  }
}
