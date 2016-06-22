package com.twitter.finagle.redis.protocol

import com.twitter.finagle.redis.ClientError
import com.twitter.finagle.redis.naggati.RedisRequestTest
import com.twitter.finagle.redis.tags.CodecTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
final class HyperLogLogCodecSuite extends RedisRequestTest {

  test("Throw a ClientError if PFADD is called with no arguments", CodecTest) {
    intercept[ClientError] {
      codec(wrap("PFADD\r\n"))
    }
  }

  test("Correctly encode PFADD with one element", CodecTest) {
    unwrap(codec(wrap("PFADD foo bar\r\n"))) {
      case PFAdd(key, List(element)) =>
        assert(key == bufFoo)
        assert(element == bufBar)
    }
  }

  test("Correctly encode PFADD with two elements", CodecTest) {
    unwrap(codec(wrap("PFADD foo bar baz\r\n"))) {
      case PFAdd(key, elements) =>
        assert(key == bufFoo)
        assert(elements(0) == bufBar)
        assert(elements(1) == bufBaz)
    }
  }

  test("Throw a ClientError if PFCOUNT is called with no arguments", CodecTest) {
    intercept[ClientError] {
      codec(wrap("PFCOUNT\r\n"))
    }
  }

  test("Correctly encode PFCOUNT with one key", CodecTest) {
    unwrap(codec(wrap("PFCOUNT foo\r\n"))) {
      case PFCount(List(key)) =>
        assert(key == bufFoo)
    }
  }

  test("Correctly encode PFCOUNT with two keys", CodecTest) {
    unwrap(codec(wrap("PFCOUNT foo bar\r\n"))) {
      case PFCount(keys) =>
        assert(keys(0) == bufFoo)
        assert(keys(1) == bufBar)
    }
  }

  test("Throw a ClientError if PFMERGE is called with no arguments", CodecTest) {
    intercept[ClientError] {
      codec(wrap("PFMERGE\r\n"))
    }
  }

  test("Throw a ClientError if PFMERGE is called with one argument", CodecTest) {
    intercept[ClientError] {
      codec(wrap("PFMERGE foo\r\n"))
    }
  }

  test("Correctly encode PFMERGE with one source key", CodecTest) {
    unwrap(codec(wrap("PFMERGE foo bar\r\n"))) {
      case PFMerge(destKey, List(srcKey)) =>
        assert(destKey == bufFoo)
        assert(srcKey == bufBar)
    }
  }

  test("Correctly encode PFMERGE with two source keys", CodecTest) {
    unwrap(codec(wrap("PFMERGE foo bar baz\r\n"))) {
      case PFMerge(destKey, srcKeys) =>
        assert(destKey == bufFoo)
        assert(srcKeys(0) == bufBar)
        assert(srcKeys(1) == bufBaz)
    }
  }
}
