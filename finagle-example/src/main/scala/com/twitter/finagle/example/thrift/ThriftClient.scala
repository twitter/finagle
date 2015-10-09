package com.twitter.finagle.example.thrift

import com.twitter.finagle.example.thriftscala.Hello
import com.twitter.finagle.Thrift

object ThriftClient {
  def main(args: Array[String]) {
    //#thriftclientapi
    val client = Thrift.newIface[Hello.FutureIface]("localhost:8080")
    client.hi().onSuccess { response =>
      println("Received response: " + response)
    }
    //#thriftclientapi
  }
}
