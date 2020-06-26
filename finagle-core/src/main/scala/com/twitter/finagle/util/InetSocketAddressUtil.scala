package com.twitter.finagle.util

import com.twitter.logging.Logger
import java.net._
import scala.collection.JavaConverters._
import scala.util.control.NonFatal

object InetSocketAddressUtil {

  type HostPort = (String, Int)

  private[this] val log = Logger()

  private[finagle] val unconnected =
    new SocketAddress { override def toString = "unconnected" }

  private[this] val anyInterfaceSupportsIpV6: Boolean = {
    try {
      val interfaces = NetworkInterface.getNetworkInterfaces().asScala
      interfaces.exists { interface =>
        val addresses = interface.getInetAddresses().asScala
        addresses.exists { inetAddress =>
          inetAddress.isInstanceOf[Inet6Address] && !inetAddress.isAnyLocalAddress() &&
            !inetAddress.isLoopbackAddress() && !inetAddress.isLinkLocalAddress()
        }
      }
    } catch {
      case NonFatal(e) => {
        log.debug(e, s"Unable to detect if any interface supports IPv6, assuming IPv4-only")
        false
      }
    }
  }

  /**
   * A proxy for InetAddress#getAllByName. Includes IPv6 Addresses only if a network interface
   * supports it
   */
  private[finagle] def getAllByName(host: String): Array[InetAddress] = {
    val allHosts = InetAddress.getAllByName(host)
    if (anyInterfaceSupportsIpV6) allHosts
    else allHosts.filter(_.isInstanceOf[Inet4Address])
  }

  /** converts 0.0.0.0 -> public ip in bound ip */
  def toPublic(bound: SocketAddress): SocketAddress = {
    bound match {
      case addr: InetSocketAddress if addr.getAddress().isAnyLocalAddress() =>
        val host =
          try InetAddress.getLocalHost()
          catch {
            case _: UnknownHostException => InetAddress.getLoopbackAddress
          }
        new InetSocketAddress(host, addr.getPort())
      case _ => bound
    }
  }

  /**
   * Parses a comma or space-delimited string of hostname and port pairs into scala pairs.
   * For example,
   *
   *     InetSocketAddressUtil.parseHostPorts("127.0.0.1:11211") => Seq(("127.0.0.1", 11211))
   *
   * @param hosts a comma or space-delimited string of hostname and port pairs.
   * @throws IllegalArgumentException if host and port are not both present
   *
   */
  def parseHostPorts(hosts: String): Seq[HostPort] =
    hosts split Array(' ', ',') filter (_.nonEmpty) map (_.split(":")) map { hp =>
      require(hp.length == 2, s"You must specify host and port in hosts: $hosts")
      hp match {
        case Array(host, "*") => (host, 0)
        case Array(host, portStr) => (host, portStr.toInt)
        case _ => throw new IllegalArgumentException("Malformed host/port specification: " + hosts)
      }
    }

  /**
   * Resolves a sequence of host port pairs into a set of socket addresses. For example,
   *
   *     InetSocketAddressUtil.resolveHostPorts(Seq(("127.0.0.1", 11211))) = Set(new InetSocketAddress("127.0.0.1", 11211))
   *
   * @param hostPorts a sequence of host port pairs
   * @throws java.net.UnknownHostException if some host cannot be resolved
   */
  def resolveHostPorts(hostPorts: Seq[HostPort]): Set[SocketAddress] =
    resolveHostPortsSeq(hostPorts).flatten.toSet

  private[finagle] def resolveHostPortsSeq(hostPorts: Seq[HostPort]): Seq[Seq[SocketAddress]] =
    hostPorts.map {
      case (host, port) =>
          getAllByName(host)
          .iterator
          .map { addr => new InetSocketAddress(addr, port) }
          .toSeq
    }

  /**
   * Parses a comma or space-delimited string of hostname and port pairs. For example,
   *
   *     InetSocketAddressUtil.parseHosts("127.0.0.1:11211") => Seq(new InetSocketAddress("127.0.0.1", 11211))
   *
   * @param hosts a comma or space-delimited string of hostname and port pairs. Or, if it is
   *          ":*" then an a single InetSocketAddress using an ephemeral port will be returned.
   *
   * @throws IllegalArgumentException if host and port are not both present
   */
  def parseHosts(hosts: String): Seq[InetSocketAddress] = {
    if (hosts == ":*") return Seq(new InetSocketAddress(0))

    parseHostPorts(hosts).map {
      case (host, port) =>
        if (host == "")
          new InetSocketAddress(port)
        else
          new InetSocketAddress(host, port)
    }
  }
}
