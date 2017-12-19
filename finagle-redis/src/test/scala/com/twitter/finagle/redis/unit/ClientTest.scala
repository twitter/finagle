package com.twitter.finagle.redis.unit

import com.twitter.finagle.Redis
import com.twitter.finagle.filter.NackAdmissionFilter
import org.scalatest.FunSuite

/**
 * Tests the functionality of the Redis client.
 */
class ClientTest extends FunSuite {
  test("client stack excludes NackAdmissionFilter") {
    val client = Redis.client
    val stack = client.stack
    assert(!stack.contains(NackAdmissionFilter.role))
  }
}
