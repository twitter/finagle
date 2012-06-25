package com.twitter.finagle.zipkin.thrift

import java.nio.ByteBuffer
import java.net.{InetAddress, InetSocketAddress, SocketAddress}
import java.util.logging.Logger
import com.twitter.finagle.thrift.thrift

/**
 * Endpoints describe a TCP endpoint that terminates RPC
 * communication.
 */
case class Endpoint(ipv4: Int, port: Short) {

  /**
   * @return If this endpoint's ip is 0.0.0.0 we get the local host and return that.
   */
  def boundEndpoint: Endpoint = if (ipv4 == 0) Endpoint(Endpoint.getLocalHost, port) else this

  def toThrift: Option[thrift.Endpoint] = {
    val e = new thrift.Endpoint
    e.setIpv4(ipv4)
    e.setPort(port)
    Some(e)
  }
}

object Endpoint {
  private[this] val log = Logger.getLogger(getClass.toString)

  val Unknown = new Endpoint(0, 0) {
    override def toThrift = None
  }

  def toIpv4(inetAddress: InetAddress): Int =
    ByteBuffer.wrap(inetAddress.getAddress).getInt

  /**
   * Get the local host as an integer.
   */
  val getLocalHost: Int = {
    try {
      Endpoint.toIpv4(InetAddress.getLocalHost)
    } catch {
      case e =>
        log.warning("Failed to retrieve local host address: %s".format(e))
        0
    }
  }

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
