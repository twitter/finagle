package com.finagle.zookeeper

import com.twitter.finagle.{ClientConnection, Service}
import com.twitter.finagle.Group
import java.net.{InetSocketAddress, SocketAddress}
import java.util.logging.Logger
import protocol.frame.{ConnectRequest, RequestHeader}

object ZookeeperClientWrapper {

  val logger = Logger.getLogger("finagle-zookeeper")

  private [this] val defaultSocket = 2181

  def main(args: Array[String]) {

    println("Hello world !")

    val testClient = this("localhost")

    testClient(None, Some(ConnectRequest.default))
      .onFailure(_ => logger.info("Failure occured"))
      .onSuccess(_ => logger.info("Success"))

  }

  def apply(host: String): Service[ZookeeperRequest, ZookeeperResponse] = {
    ZookeeperClient.newClient(Group[SocketAddress](new InetSocketAddress(host, defaultSocket))).toService
  }

}
