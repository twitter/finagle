package com.twitter.finagle.example.redis

import com.twitter.finagle.redis.Client
import com.twitter.finagle.redis.util.RedisCluster
import com.twitter.io.Buf
import com.twitter.util.Await

object RedisClient {

  def main(args: Array[String]): Unit = {

    println("Starting Redis instance...")
    RedisCluster.start(1)

    val client = Client(RedisCluster.hostAddresses())
    println("Setting foo -> bar...")
    client.set(Buf.Utf8("foo"), Buf.Utf8("bar"))
    println("Getting value for key 'foo'")

    val getResult = Await.result(client.get(Buf.Utf8("foo")))
    getResult match {
      case Some(Buf.Utf8(s)) => println("Got result: " + s)
      case None => println("Didn't get the value!")
    }

    println("Closing client...")
    client.close()
    println("Stopping Redis instance...")
    RedisCluster.stop()
    println("Done!")
  }

}
