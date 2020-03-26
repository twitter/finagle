package com.twitter.finagle.example.thrift

import com.twitter.finagle.example.thriftscala.Hello
import com.twitter.finagle.Thrift
import com.twitter.util.Await

object ThriftClient {
  def main(args: Array[String]): Unit = {
    //#thriftclientapi
    val client = Thrift.client.build[Hello.MethodPerEndpoint]("localhost:8080")
    val response = client.hi().onSuccess { response => println("Received response: " + response) }
    Await.result(response)
    //#thriftclientapi
  }
}
