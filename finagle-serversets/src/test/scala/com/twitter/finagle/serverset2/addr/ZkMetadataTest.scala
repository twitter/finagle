package com.twitter.finagle.serverset2.addr

import com.twitter.finagle.stats.InMemoryStatsReceiver
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

    val sr = new InMemoryStatsReceiver
    val fallback = new Ordering[Address] {
      def compare(a0: Address, a1: Address) = 0
    }
    val order = ZkMetadata.addressOrdering(sr, fallback)

    val sorted = addresses.sorted(order)
    sorted.indices.foreach { i => assert(addresses(i) == sorted(size - i - 1)) }

    assert(sr.counters(Seq("zk_metadata")) == 9)

    val noMetaData: Seq[Address] = addresses.collect {
      case a@Address.Inet(_, _) => a.copy(metadata = Addr.Metadata.empty)
    }
    val sortedNoMetaData = noMetaData.sorted(order)
    sortedNoMetaData.indices.foreach { i => assert(noMetaData(i) == sortedNoMetaData(i)) }

    assert(sr.counters(Seq("fallback")) == 9)
  }
}
