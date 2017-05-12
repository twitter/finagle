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

  test("AddressOrdering") {
    val size = 10
    val addresses: Seq[Address] = (size until 0 by -1).map { i =>
      val metadata = ZkMetadata.toAddrMetadata(ZkMetadata(Some(i)))
      Address.Inet(InetSocketAddress.createUnresolved("test", 0), metadata)
    }

    val sorted = addresses.sorted(ZkMetadata.AddressOrdering)
    sorted.indices.foreach { i => assert(addresses(i) == sorted(size - i - 1)) }

    val noMetaData: Seq[Address] = addresses.collect {
      case a@Address.Inet(_, _) => a.copy(metadata = Addr.Metadata.empty)
    }
    val sortedNoMetaData = noMetaData.sorted(ZkMetadata.AddressOrdering)
    sortedNoMetaData.indices.foreach { i => assert(noMetaData(i) == sortedNoMetaData(i)) }
  }

  test("Heterogenous AddressOrdering") {
    def newAddress(shardId: Option[Int]): Address = {
      val metadata = ZkMetadata.toAddrMetadata(ZkMetadata(shardId))
      Address.Inet(InetSocketAddress.createUnresolved("test", 0), metadata)
    }

    val addresses: Seq[Address] = Seq(
      newAddress(None),
      newAddress(Some(3)),
      newAddress(Some(2)),
      newAddress(Some(1)),
      Address.Inet(InetSocketAddress.createUnresolved("test", 0), Addr.Metadata.empty),
      Address.Failed(new Exception))

    val sorted = addresses.zipWithIndex.sortBy(_._1)(ZkMetadata.AddressOrdering)
    assert(sorted.map(_._2) == Seq(3, 2, 1, 0, 4, 5))
  }
}
