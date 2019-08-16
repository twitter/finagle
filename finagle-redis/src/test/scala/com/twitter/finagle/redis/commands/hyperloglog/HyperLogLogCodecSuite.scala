package com.twitter.finagle.redis.protocol

import com.twitter.finagle.redis.RedisRequestTest
import com.twitter.finagle.redis.tags.CodecTest

final class HyperLogLogCodecSuite extends RedisRequestTest {

  test("PFADD", CodecTest) { checkSingleKeyMultiVal("PFADD", PFAdd.apply) }
  test("PFCOUNT", CodecTest) { checkMultiKey("PFCOUNT", PFCount.apply) }
  test("PFMERGE", CodecTest) { checkSingleKeyMultiVal("PFMERGE", PFMerge.apply) }

}
