package com.twitter.finagle.partitioning

import com.twitter.finagle.{Addr, Address}
import java.net.{InetAddress, InetSocketAddress}
import org.scalatest.funsuite.AnyFunSuite

class PartitionNodeTest extends AnyFunSuite {

  test("fromInetSocketAddress to PartitionNode") {
    val isa1 = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
    val partitionNode1 = PartitionNode.fromInetSocketAddress(isa1)
    assert(partitionNode1 == PartitionNode("localhost", 0, 1, None))

    val metadata = PartitionNodeMetadata.toAddrMetadata(PartitionNodeMetadata(1, Some("2")))
    val isa2 = new InetSocketAddress(InetAddress.getLoopbackAddress, 1)
    val partitionNode2 = PartitionNode.fromInetSocketAddress(isa2, metadata)
    assert(partitionNode2 == PartitionNode("localhost", 1, 1, Some("2")))
  }

  test("fromAddress to PartitionNode") {
    val metadata = PartitionNodeMetadata.toAddrMetadata(PartitionNodeMetadata(1, Some("3")))
    val addr = Address.Inet(new InetSocketAddress(InetAddress.getLoopbackAddress, 0), metadata)
    val Some(partitionNode) = PartitionNode.fromAddress(addr)
    assert(partitionNode == PartitionNode("localhost", 0, 1, Some("3")))
  }

  test("PartitionNode to Address") {
    val partitionNode = PartitionNode("localhost", 500, 1, Some("4"))
    val addr = PartitionNode.toAddress(partitionNode)
    assert(addr.isInstanceOf[Address.Inet])
    val inetAddr = addr.asInstanceOf[Address.Inet]
    assert(inetAddr.addr == new InetSocketAddress(InetAddress.getLoopbackAddress, 500))
    assert(
      inetAddr.metadata == Addr.Metadata(
        "cache_node_metadata" -> PartitionNodeMetadata(1, Some("4"))))
  }
}
