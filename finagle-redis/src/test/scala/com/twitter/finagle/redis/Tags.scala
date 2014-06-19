package com.twitter.finagle.redis.tags

import org.scalatest.Tag

object RedisTest extends Tag("com.twitter.finagle.redis.tags.RedisTest")
object IntegrationTest extends Tag("com.twitter.finagle.redis.tags.IntegrationTest")
object SlowTest extends Tag("com.twitter.finagle.redis.tags.SlowTest")
object CodecTest extends Tag("com.twitter.finagle.redis.tags.CodecTest")
