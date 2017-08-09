package com.twitter.finagle.serverset2.addr

import com.twitter.finagle.{Addr, Address}
import java.net.InetSocketAddress
import org.scalatest.FunSuite

class ZkMetadataTest extends FunSuite {
  val metadata = ZkMetadata(Some(4))

  test("toAddrMetadata") {
    val addrMetadata = ZkMetadata.toAddrMetadata(metadata)
    assert(addrMetadata(ZkMetadata.key) == metadata)
  }

  test("fromAddrMetadata") {
    val addrMetadata = ZkMetadata.toAddrMetadata(metadata)
    assert(ZkMetadata.fromAddrMetadata(addrMetadata) == Some(metadata))
  }

  test("fromAddrMetadata preserves None ZkMetadata") {
    assert(ZkMetadata.fromAddrMetadata(Addr.Metadata.empty) == None)
  }

  test("fromAddrMetadata preserves ZkMetadata with None shardid") {
    val addrMetadata = ZkMetadata.toAddrMetadata(ZkMetadata(None))
    assert(ZkMetadata.fromAddrMetadata(addrMetadata) == Some(ZkMetadata(None)))
  }

  test("shardHashOrdering") {
    val size = 10
    val addresses: Seq[Address] = (0 until size).map { i =>
      val metadata = ZkMetadata.toAddrMetadata(ZkMetadata(Some(i)))
      Address.Inet(InetSocketAddress.createUnresolved("test", 0), metadata)
    }
    def sort(addrs: Seq[Address]): Seq[Address] = addrs.sorted(ZkMetadata.shardHashOrdering)
    // deterministic
    assert(sort(addresses) == sort(addresses))
    assert(sort(addresses) != addresses)

    def newAddress(shardId: Option[Int]): Address = {
      val metadata = ZkMetadata.toAddrMetadata(ZkMetadata(shardId))
      Address.Inet(InetSocketAddress.createUnresolved("test", 0), metadata)
    }

    val heterogenous: Seq[Address] = Seq(
      newAddress(None),
      newAddress(Some(3)),
      newAddress(Some(2)),
      newAddress(Some(1)),
      Address.Inet(InetSocketAddress.createUnresolved("test", 0), Addr.Metadata.empty),
      Address.Failed(new Exception)
    )

    assert(heterogenous.sorted(ZkMetadata.shardHashOrdering) != heterogenous)
  }
}
