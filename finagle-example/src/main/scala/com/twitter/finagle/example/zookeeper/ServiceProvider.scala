package com.twitter.finagle.example.zookeeper

import java.net.InetSocketAddress

import com.twitter.finagle.Service
import com.twitter.finagle.builder.{Server, ServerBuilder}
import com.twitter.finagle.http._
import com.twitter.util.{Future, Return, Throw}

/**
  * This demo shows how to register your service in zookeeper.
  * It assumes your zookeeper runs on your local machine with port 2181, 2182 and 2183.
  * You can change zookeeperDest to your zookeeper ip and port.
  *
  * Run ServiceProvider before ServiceConsumer
  *
  */
object ServiceProvider {

  //change it to your zookeeper ip and port
  val zookeeperDest = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183"

  /**
    * The echo server simply return the request content.
    */
  class EchoServer extends Service[Request, Response] {
    def apply(request: Request) = {
      val response = Response(Version.Http11, Status.Ok)
      response.contentString = request.getContentString()
      Future.value(response)
    }
  }

  object EchoServer {
    val servicePath = "/services/echo"
  }

  def main(args: Array[String]): Unit = {

    //start your service on port 8080
    val server: Server = ServerBuilder()
      .codec(Http())
      .bindTo(new InetSocketAddress(8080))
      .name("echoserver")
      .build(new EchoServer)

    //providerPath: "zk!127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183!/services/echo/!0"
    //syntax:        zk!host!path!shardId
    //zk:            schema name
    //host:          zookeeper connection string
    //path:          service registration path in zookeeper
    //shardId:       it's used internally by Twitter, can be set to 0 in most cases.
    val echoServicePath = buildProviderPath(EchoServer.servicePath)

    server.announce(echoServicePath) onSuccess { _ =>
      println("service register success")
    } onFailure { e =>
      println(s"error: $e")
    }
  }

  def buildProviderPath(servicePath: String, zookeeperDest: String = zookeeperDest): String = {
    s"zk!$zookeeperDest!$servicePath!0"
  }

  def buildConsumerPath(servicePath: String, zookeeperDest: String = zookeeperDest): String = {
    s"zk!$zookeeperDest!$servicePath"
  }

}
