package com.twitter.finagle.partitioning

import com.twitter.finagle.{Addr, Address}
import java.net.{InetSocketAddress, SocketAddress}

// Type definition representing a partition node
case class PartitionNode(host: String, port: Int, weight: Int, key: Option[String] = None)
    extends SocketAddress {
  // Use overloads to keep the same ABI
  def this(host: String, port: Int, weight: Int) = this(host, port, weight, None)
}

object PartitionNode {

  /**
   * Utility method for translating a `PartitionNode` to an `Address`
   * (used when constructing a `Name` representing a `Cluster`).
   */
  private[finagle] val toAddress: PartitionNode => Address = {
    case PartitionNode(host, port, weight, key) =>
      val metadata = PartitionNodeMetadata.toAddrMetadata(PartitionNodeMetadata(weight, key))
      Address.Inet(new InetSocketAddress(host, port), metadata)
  }

  /**
   * Translating from an InetSocketAddress to a PartitionNode
   */
  private[finagle] def fromInetSocketAddress(
    isa: InetSocketAddress,
    metadata: Addr.Metadata = Addr.Metadata.empty
  ): PartitionNode = {
    val PartitionNodeMetadata(weight, key) =
      PartitionNodeMetadata.fromAddrMetadata(metadata).getOrElse(PartitionNodeMetadata(1, None))
    new PartitionNode(isa.getHostName, isa.getPort, weight, key)
  }

  /**
   * Translating from an Address to a PartitionNode
   */
  private[finagle] def fromAddress(address: Address): Option[PartitionNode] = {
    address match {
      case Address.Inet(addr, metadata) =>
        val PartitionNodeMetadata(weight, key) =
          PartitionNodeMetadata.fromAddrMetadata(metadata).getOrElse(PartitionNodeMetadata(1, None))
        Some(new PartitionNode(addr.getHostName, addr.getPort, weight, key))
      case _ => None
    }
  }
}
