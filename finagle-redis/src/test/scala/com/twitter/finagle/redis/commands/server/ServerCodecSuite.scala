package com.twitter.finagle.redis.protocol

import com.twitter.finagle.redis.RedisRequestTest
import com.twitter.finagle.redis.tags.CodecTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
final class ServerCodecSuite extends RedisRequestTest {

  test("FLUSHALL", CodecTest) {
    assert(encodeCommand(FlushAll) == Seq("FLUSHALL"))
  }
}
