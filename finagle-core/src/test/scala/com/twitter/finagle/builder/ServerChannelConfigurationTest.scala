package com.twitter.finagle.builder

import com.twitter.conversions.time._
import com.twitter.finagle.ChannelClosedException
import com.twitter.finagle.client.StringClient
import com.twitter.finagle.server.StringServer
import com.twitter.finagle.Service
import com.twitter.util.{Await, Future}
import java.net.{InetAddress, InetSocketAddress}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ServerChannelConfigurationTest
  extends FunSuite
  with StringClient
  with StringServer {

  val identityService = Service.mk[String, String] { req => Future.value(req) }

  test("close connection after max life time duration") {
    val lifeTime = 1.seconds
    val address = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
    val server = ServerBuilder()
      .stack(Server())
      .bindTo(address)
      .name("FinagleServer")
      .hostConnectionMaxLifeTime(lifeTime)
      .build(identityService)

    val client: Service[String, String] = ClientBuilder()
      .stack(Client(appendDelimeter = false))
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
    val idleTime = 1.second
    val address = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
    val server = ServerBuilder()
      .stack(Server())
      .bindTo(address)
      .name("FinagleServer")
      .hostConnectionMaxIdleTime(idleTime)
      .build(identityService)

    val client: Service[String, String] = ClientBuilder()
      .stack(Client(appendDelimeter = false))
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
