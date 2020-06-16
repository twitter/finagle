package com.twitter.finagle.example.thrift

import com.twitter.finagle.example.thriftscala.Hello
import com.twitter.finagle.Thrift
import com.twitter.util.{Await, Future}

object ThriftServer {
  def main(args: Array[String]): Unit = {
    //#thriftserverapi
    val server = Thrift.server.serveIface(
      "localhost:8080",
      new Hello.MethodPerEndpoint {
        def hi() = Future.value("hi")
      })
    Await.ready(server)
    //#thriftserverapi
  }
}
