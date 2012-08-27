package com.twitter.finagle.example.redis

import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.redis.{Client, Redis}
import com.twitter.finagle.redis.util.{RedisCluster, StringToChannelBuffer}

object RedisClient {

  def main(args: Array[String]) {

    println("Starting Redis instance...")
    RedisCluster.start(1)

    val client = Client(RedisCluster.hostAddresses())
    println("Setting foo -> bar...")
    client.set(StringToChannelBuffer("foo"), StringToChannelBuffer("bar"))
    println("Getting value for key 'foo'")
    val getResult = client.get(StringToChannelBuffer("foo"))()
    getResult match {
      case Some(n) => println("Got result: " + new String(n.array))
      case None => println("Didn't get the value!")
    }

    println("Closing client...")
    client.release()
    println("Stopping Redis instance...")
    RedisCluster.stop()
    println("Done!")
  }

}
