package com.twitter.finagle.redis.protocol

import com.twitter.finagle.redis.RedisRequestTest
import com.twitter.finagle.redis.tags.CodecTest

final class SetCodecSuite extends RedisRequestTest {

  test("SADD", CodecTest) { checkSingleKeyMultiVal("SADD", SAdd.apply) }
  test("SINTER", CodecTest) { checkMultiKey("SINTER", SInter.apply) }

}
