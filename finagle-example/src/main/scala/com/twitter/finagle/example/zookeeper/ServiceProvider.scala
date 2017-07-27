package com.twitter.finagle.example.zookeeper

import java.net.InetSocketAddress

import com.twitter.finagle.http.{Request, Response, Status, Version}
import com.twitter.finagle.{Http, Service}
import com.twitter.util.{Await, Future}

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

    //echoServicePath: zk!127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183!/services/echo/!0
    //syntax:          schema!host!path!shardId
    //schema:          for server, use zk, for client, use zk2
    //host:            zookeeper connection string
    //path:            service registration path in zookeeper
    //shardId:         it's used internally by Twitter, can be set to 0 in most cases
    val echoServicePath = buildProviderPath(EchoServer.servicePath)

    //start your service on port 8080 and register to zookeeper
    Await.ready(
      Http.server
        .withLabel("echo-server")
        .serveAndAnnounce(echoServicePath, new InetSocketAddress(8080), new EchoServer)
    )
  }

  def buildProviderPath(servicePath: String, zookeeperDest: String = zookeeperDest): String = {
    s"zk!$zookeeperDest!$servicePath!0"
  }

  def buildConsumerPath(servicePath: String, zookeeperDest: String = zookeeperDest): String = {
    s"zk2!$zookeeperDest!$servicePath"
  }

}
