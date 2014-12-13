package com.twitter.finagle.zipkin.thrift

import com.twitter.finagle.thrift.thrift
import com.twitter.util.NonFatal
import java.net.{InetAddress, InetSocketAddress, SocketAddress}
import java.nio.ByteBuffer
import java.util.logging.Logger

/**
 * Endpoints describe a TCP endpoint that terminates RPC
 * communication.
 */
case class Endpoint(ipv4: Int, port: Short) {

  /**
   * @return If this endpoint's ip is 0.0.0.0 or 127.0.0.1 we get the local host and return that.
   */
  def boundEndpoint: Endpoint = {
    if (ipv4 == 0 || ipv4 == Endpoint.Loopback) this.copy(ipv4=Endpoint.getLocalHost) else this
  }

  def toThrift: thrift.Endpoint = {
    val e = new thrift.Endpoint
    e.setIpv4(ipv4)
    e.setPort(port)
    e
  }
}

object Endpoint {
  private[this] val log = Logger.getLogger(getClass.toString)

  val Loopback = Endpoint.toIpv4(InetAddress.getByAddress(Array[Byte](127,0,0,1)))

  val Unknown = new Endpoint(0, 0)

  val Local = {
    try {
      val ipv4 = Endpoint.toIpv4(InetAddress.getLoopbackAddress)
      Endpoint(ipv4,0)
    } catch {
      case NonFatal(_) => Endpoint.Unknown
    }
  }

  def toIpv4(inetAddress: InetAddress): Int =
    ByteBuffer.wrap(inetAddress.getAddress).getInt

  /**
   * Get the local host as an integer.
   */
  lazy val getLocalHost: Int = Local.ipv4

  /**
   * @return If possible, convert from a SocketAddress object to an Endpoint.
   * If not, return Unknown Endpoint.
   */
  def fromSocketAddress(socketAddress: SocketAddress): Endpoint = {
    socketAddress match {
      case inet: InetSocketAddress => {
        Endpoint(toIpv4(inet.getAddress), inet.getPort.toShort)
      }
      case _ => Endpoint.Unknown
    }
  }
}
