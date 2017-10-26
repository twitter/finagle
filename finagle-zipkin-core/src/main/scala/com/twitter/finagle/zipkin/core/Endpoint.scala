package com.twitter.finagle.zipkin.core

import com.twitter.finagle.thrift.thrift
import java.net.{InetAddress, InetSocketAddress, SocketAddress}
import java.nio.ByteBuffer
import scala.util.control.NonFatal

/**
 * Endpoints describe a TCP endpoint that terminates RPC
 * communication.
 */
case class Endpoint(ipv4: Int, port: Short) {

  /**
   * @return If this endpoint's ip is 0.0.0.0 or 127.0.0.1 we get the local host and return that.
   */
  def boundEndpoint: Endpoint = {
    if (ipv4 == 0 || ipv4 == Endpoint.Loopback) this.copy(ipv4 = Endpoint.getLocalHost) else this
  }

  def toThrift: thrift.Endpoint = {
    val e = new thrift.Endpoint
    e.setIpv4(ipv4)
    e.setPort(port)
    e
  }
}

object Endpoint {
  val Loopback: Int = Endpoint.toIpv4(InetAddress.getByAddress(Array[Byte](127, 0, 0, 1)))

  val Unknown: Endpoint = new Endpoint(0, 0)

  val Local: Endpoint = {
    try {
      val ipv4 = Endpoint.toIpv4(InetAddress.getLoopbackAddress)
      Endpoint(ipv4, 0)
    } catch {
      case NonFatal(_) => Endpoint.Unknown
    }
  }

  /**
   * Returns `0` if `inetAddress` is null, which will be the
   * case for unresolved `InetSocketAddress`-es.
   */
  def toIpv4(inetAddress: InetAddress): Int = {
    if (inetAddress == null) 0
    else ByteBuffer.wrap(inetAddress.getAddress).getInt
  }

  /**
   * Get the local host as an integer.
   */
  lazy val getLocalHost: Int = Local.ipv4

  /**
   * @return If possible, convert from a SocketAddress object to an Endpoint.
   * If not, return `Unknown` Endpoint.
   */
  def fromSocketAddress(socketAddress: SocketAddress): Endpoint = {
    socketAddress match {
      case inet: InetSocketAddress =>
        Endpoint(toIpv4(inet.getAddress), inet.getPort.toShort)
      case _ => Endpoint.Unknown
    }
  }
}
