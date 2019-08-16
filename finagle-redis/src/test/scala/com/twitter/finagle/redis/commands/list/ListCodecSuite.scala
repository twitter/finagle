package com.twitter.finagle.redis.protocol

import com.twitter.finagle.redis.RedisRequestTest
import com.twitter.finagle.redis.tags.CodecTest

final class ListCodecSuite extends RedisRequestTest {

  test("LPUSH", CodecTest) { checkSingleKeyMultiVal("LPUSH", LPush.apply) }

}
