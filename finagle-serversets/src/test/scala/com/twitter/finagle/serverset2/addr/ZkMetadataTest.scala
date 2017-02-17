package com.twitter.finagle.serverset2.addr

import com.twitter.finagle.Addr
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
}
