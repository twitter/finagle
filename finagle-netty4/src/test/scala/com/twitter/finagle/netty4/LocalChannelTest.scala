package com.twitter.finagle.netty4

import com.twitter.finagle.{Service, Stack}
import com.twitter.finagle.client.utils.StringClient.StringClientPipeline
import com.twitter.finagle.netty4.ssl.Netty4SslTestComponents._
import com.twitter.finagle.server.utils.StringServer
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Await, Awaitable, Future, Duration}
import io.netty.channel.local.LocalAddress
import org.scalatest.funsuite.AnyFunSuite

/**
 * This test class uses Netty's `LocalAddress` to signal
 * to Finagle client and server components that it should
 * use a `LocalChannel` and `LocalServerChannel` respectively.
 * Using a `LocalChannel` allows for testing of functionality
 * without using actual sockets.
 */
class LocalChannelTest extends AnyFunSuite {

  private[this] def await[T](a: Awaitable[T]): T = Await.result(a, Duration.fromSeconds(1))

  private[this] val service = Service.mk[String, String](Future.value)

  // To use a full Finagle client (i.e. StringClient) here
  // requires additional changes to `c.t.f.Address` to allow
  // it to accept a `SocketAddress` and not just an `InetSocketAddress`.
  // So, we test components of the client here instead as a substitute
  // for the time being.
  test("client components and server can communicate") {
    val addr = new LocalAddress("LocalChannelTestPlain")
    val server = StringServer.server.serve(addr, service)
    try {
      val transporter =
        Netty4Transporter.raw[String, String](StringClientPipeline, addr, Stack.Params.empty)
      val transport = await(transporter())
      transport.write("Finagle")
      try {
        val result = await(transport.read())
        assert(result == "Finagle")
      } finally {
        transport.close()
      }
    } finally {
      server.close()
    }
  }

  test("client components and server can communicate over SSL/TLS") {
    val addr = new LocalAddress("LocalChannelTestTls")
    val server = StringServer.server.withTransport.tls(serverConfig).serve(addr, service)
    try {
      val transporter =
        Netty4Transporter.raw[String, String](
          StringClientPipeline,
          addr,
          Stack.Params.empty + Transport.ClientSsl(Some(clientConfig)))
      val transport = await(transporter())
      transport.write("Finagle over TLS")
      try {
        val result = await(transport.read())
        assert(result == "Finagle over TLS")
      } finally {
        transport.close()
      }
    } finally {
      server.close()
    }
  }

}
