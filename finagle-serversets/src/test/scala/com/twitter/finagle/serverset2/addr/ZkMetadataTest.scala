package com.twitter.finagle.serverset2.addr

import com.twitter.finagle.Addr
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite

@RunWith(classOf[JUnitRunner])
class ZkMetadataTest extends FunSuite {
  val metadata = ZkMetadata(Some(4))

  test("toAddrMetadata") {
    val addrMetadata = ZkMetadata.toAddrMetadata(metadata)
    assert(addrMetadata(ZkMetadata.key) == metadata)
  }

  test("fromAddrMetadata") {
    val addrMetadata = ZkMetadata.toAddrMetadata(metadata)
    assert(ZkMetadata.fromAddrMetadata(addrMetadata) == metadata)
  }

  test("unapply") {
    Addr.Metadata.empty match {
      case ZkMetadata(_) => assert(false)
      case _ => assert(true)
    }
    ZkMetadata.toAddrMetadata(metadata) match {
      case ZkMetadata(4) => assert(true)
      case _ => assert(false)
    }
  }
}
