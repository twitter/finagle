package com.twitter.finagle.redis.param

import com.twitter.finagle.Stack
import com.twitter.hashing

object RedisKeyHasher {
  // murmur3 hash was picked because it's very fast, it has a reasonably good
  // distribution, and it's not very collision prone. it's not difficult to
  // reverse though, so it shouldn't be used as a cryptographic hash.
  implicit val param: Stack.Param[RedisKeyHasher] =
    Stack.Param(RedisKeyHasher(hashing.KeyHasher.MURMUR3))
}

case class RedisKeyHasher(hasher: hashing.KeyHasher) {
  def mk(): (RedisKeyHasher, Stack.Param[RedisKeyHasher]) =
    (this, RedisKeyHasher.param)
}
