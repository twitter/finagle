package com.twitter.finagle.partitioning

import com.twitter.finagle.Address
import java.net.{InetSocketAddress, SocketAddress}

// Type definition representing a cache node
case class CacheNode(host: String, port: Int, weight: Int, key: Option[String] = None)
    extends SocketAddress {
  // Use overloads to keep the same ABI
  def this(host: String, port: Int, weight: Int) = this(host, port, weight, None)
}

object CacheNode {

  /**
   * Utility method for translating a `CacheNode` to an `Address`
   * (used when constructing a `Name` representing a `Cluster`).
   */
  private[finagle] val toAddress: CacheNode => Address = {
    case CacheNode(host, port, weight, key) =>
      val metadata = CacheNodeMetadata.toAddrMetadata(CacheNodeMetadata(weight, key))
      Address.Inet(new InetSocketAddress(host, port), metadata)
  }
}
