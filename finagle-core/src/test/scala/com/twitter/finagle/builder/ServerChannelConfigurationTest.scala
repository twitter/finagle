package com.twitter.finagle.builder

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.ChannelClosedException
import com.twitter.finagle.Service
import com.twitter.finagle.client.utils.StringClient
import com.twitter.finagle.server.utils.StringServer
import com.twitter.util.{Await, Future}
import java.net.{InetAddress, InetSocketAddress}
import org.scalatest.funsuite.AnyFunSuite

class ServerChannelConfigurationTest extends AnyFunSuite {

  val identityService = Service.mk[String, String] { req => Future.value(req) }

  test("close connection after max life time duration") {
    val lifeTime = 100.millis
    val address = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
    val server = StringServer
      .Server()
      .withSession.maxLifeTime(lifeTime)
      .withLabel("FinagleServer")
      .serve(address, identityService)

    val client: Service[String, String] = ClientBuilder()
      .stack(StringClient.Client(appendDelimiter = false))
      .daemon(true) // don't create an exit guard
      .hosts(server.boundAddress.asInstanceOf[InetSocketAddress])
      .hostConnectionLimit(1)
      .build()

    // Issue a request which is NOT newline-delimited. Server should close connection
    // after waiting `lifeTime` for a new line
    intercept[ChannelClosedException] { Await.result(client("123"), lifeTime * 3) }
    server.close()
  }

  test("close connection after max idle time duration") {
    val idleTime = 100.millis
    val address = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
    val server = StringServer
      .Server()
      .withSession.maxIdleTime(idleTime)
      .withLabel("FinagleServer")
      .serve(address, identityService)

    val client: Service[String, String] = ClientBuilder()
      .stack(StringClient.Client(appendDelimiter = false))
      .daemon(true) // don't create an exit guard
      .hosts(server.boundAddress.asInstanceOf[InetSocketAddress])
      .hostConnectionLimit(1)
      .build()

    // Issue a request which is NOT newline-delimited. Server should close connection
    // after waiting `idleTime` for a new line
    intercept[ChannelClosedException] { Await.result(client("123"), idleTime * 3) }
    server.close()
  }
}
