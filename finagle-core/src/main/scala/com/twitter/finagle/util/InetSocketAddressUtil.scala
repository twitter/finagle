package com.twitter.finagle.util

import java.net.{InetAddress, InetSocketAddress, SocketAddress, UnknownHostException}

object InetSocketAddressUtil {

  type HostPort = (String, Int)

  private[finagle] val unconnected =
    new SocketAddress { override def toString = "unconnected" }

  /** converts 0.0.0.0 -> public ip in bound ip */
  def toPublic(bound: SocketAddress): SocketAddress = {
    bound match {
      case addr: InetSocketAddress if addr.getAddress().isAnyLocalAddress() =>
        val host = try InetAddress.getLocalHost() catch {
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
      require(hp.length == 2, "You must specify host and port")
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
    hostPorts map { case (host, port) =>
      (InetAddress.getAllByName(host) map { addr =>
        new InetSocketAddress(addr, port)
      }).toSeq
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

    (parseHostPorts(hosts) map { case (host, port) =>
      if (host == "")
        new InetSocketAddress(port)
      else
        new InetSocketAddress(host, port)
    }).toList
  }
}
