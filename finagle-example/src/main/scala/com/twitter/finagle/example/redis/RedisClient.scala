package com.twitter.finagle.example.redis

/* Temporarily disabled due to build issues

import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.redis.{Client, Redis}
import com.twitter.finagle.redis.util.RedisCluster

object RedisClient {

  def main(args: Array[String]) {

    println("Starting Redis instance...")
    RedisCluster.start(1)

    val client = Client(RedisCluster.hostAddresses())
    println("Setting foo -> bar...")
    client.set("foo", "bar".getBytes)
    println("Getting value for key 'foo'")
    val getResult = client.get("foo")
    // client returns a Future containing the value byte array, so we extract the Option object
    val optionResult = getResult.apply()
    // check the option object for the result
    optionResult match {
      case Some(n) => println("Got result: " + new String(n))
      case None => println("Didn't get the value!")
    }

    println("Closing client...")
    client.release()
    println("Stopping Redis instance...")
    RedisCluster.stop()
    println("Done!")
  }

}

*/
