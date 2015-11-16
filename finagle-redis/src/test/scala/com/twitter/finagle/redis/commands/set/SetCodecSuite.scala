package com.twitter.finagle.redis.protocol

import com.twitter.finagle.redis.ClientError
import com.twitter.finagle.redis.naggati.RedisRequestTest
import com.twitter.finagle.redis.tags.CodecTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
final class SetCodecSuite extends RedisRequestTest {

  test("Correctly encode SADD", CodecTest) {
    unwrap(codec(wrap("SADD foo bar\r\n"))) {
      case SAdd(key, List(member)) => {
        assert(key == foo)
        assert(member == bar)
      }
    }
  }

  test("Throw a ClientError if SINTER is called with no key", CodecTest) {
    intercept[ClientError] {
      codec(wrap("SINTER\r\n"))
    }
  }

  test("Correctly encode SINTER for one key", CodecTest) {
    unwrap(codec(wrap("SINTER foo\r\n"))) {
      case SInter(keys) => {
        assert(keys(0) == foo)
      }
    }
  }

  test("Correctly encode SINTER for two keys", CodecTest) {
    unwrap(codec(wrap("SINTER foo bar\r\n"))) {
      case SInter(keys) => {
        assert(keys(0) == foo)
        assert(keys(1) == bar)
      }
    }
  }
}
