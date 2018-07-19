package com.twitter.finagle.serverset2.addr

import com.twitter.finagle.{Addr, Address}
import java.net.InetSocketAddress
import org.scalacheck.Gen
import org.scalatest.FunSuite
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import scala.util.Random

class ZkMetadataTest extends FunSuite with GeneratorDrivenPropertyChecks {
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

  private def buildAddress(address: String, port: Int, shardMetaData: Option[Option[Int]]): Address = {
    val socketAddress = new InetSocketAddress(address, port)
    val metadata = shardMetaData match {
      case Some(v) => ZkMetadata.toAddrMetadata(ZkMetadata(v))
      case None => Addr.Metadata.empty
    }
    Address.Inet(socketAddress, metadata)
  }

  private def addressFromMetadata(metaDataGen: Gen[Option[Option[Int]]]): Gen[Address] = {
    for {
      addrOctet <- Gen.choose[Int](1, 5)
      port <- Gen.choose[Int](1, 5)
      meta <- metaDataGen
    } yield buildAddress("1.2.3." + addrOctet, port, meta)
  }

  test("transitivity: address with shard id's are always before addresses without") {
    // some specific cases
    val noShardId = buildAddress("1.2.3.2", 1, Some(None))
    val before = Seq(
      buildAddress("1.2.3.1", 1, Some(Some(1))),
      buildAddress("1.2.3.2", 1, Some(Some(1)))
    )

    before.foreach { a =>
      assert(ZkMetadata.shardHashOrdering.compare(a, noShardId) < 0)
      assert(ZkMetadata.shardHashOrdering.compare(noShardId, a) > 0)
    }

    // property based tests
    val addrGen = addressFromMetadata(Gen.oneOf(
      Gen.const(None),
      Gen.const(Some(None))))

    val withShardId = buildAddress("1.2.3.3", 3, Some(Some(1)))
    forAll(addrGen) { addr =>
      assert(ZkMetadata.shardHashOrdering.compare(withShardId, addr) < 0)
      assert(ZkMetadata.shardHashOrdering.compare(addr, withShardId) > 0)
    }
  }

  test("transitivity: address with metadata always before addresses without") {
    // some specific cases
    val base = buildAddress("1.2.3.2", 1, None)
    val before = Seq(
      buildAddress("1.2.3.1", 1, Some(None)),
      buildAddress("1.2.3.1", 1, Some(Some(1))),
      buildAddress("1.2.3.2", 1, Some(None)),
      buildAddress("1.2.3.2", 1, Some(Some(1)))
    )

    before.foreach { a =>
      assert(ZkMetadata.shardHashOrdering.compare(a, base) < 0)
      assert(ZkMetadata.shardHashOrdering.compare(base, a) > 0)
    }

    // property based tests
    val withMetadata =addressFromMetadata(Gen.oneOf(
      Gen.const(Some(Some(1))),
      Gen.const(Some(None))))

    val withoutMetadata =addressFromMetadata(Gen.const(None))

    val toCompare = for {
      w <- withMetadata
      wo <- withoutMetadata
    } yield (w, wo)

    forAll(toCompare) { case (w, wo) =>
      assert(ZkMetadata.shardHashOrdering.compare(w, wo) < 0)
      assert(ZkMetadata.shardHashOrdering.compare(wo, w) > 0)
    }
  }

  test("transitivity: sorting properties for a ips") {
    val addrs = Seq(
      buildAddress("1.2.3.3", 0, Some(Some(2))),
      buildAddress("1.2.3.4", 0, Some(None)),
      buildAddress("1.2.3.4", 0, None),
      buildAddress("1.2.3.5", 0, Some(Some(2)))
    )

    assert(addrs.sorted(ZkMetadata.shardHashOrdering) ==
      Seq(
        buildAddress("1.2.3.5", 0, Some(Some(2))),
        buildAddress("1.2.3.3", 0, Some(Some(2))), // Second based on hash of ip
        buildAddress("1.2.3.4", 0, Some(None)),
        buildAddress("1.2.3.4", 0, None)
      ))
  }

  test("transitivity: no blowups and ordering is unique") {
    // We intentionally limit the domain of generated address, shard ids,
    // and ports to promote collisions that will exercise other parts of
    // the comparison.
    val addrGen = addressFromMetadata(
      Gen.oneOf(
        Gen.const(None),
        Gen.const(Some(None)),
        Gen.choose[Int](1, 5).map(id => Some(Some(id)))
      )
    )

    forAll(Gen.listOf(addrGen)) { addrs =>
      // There should be a unique ordering for addresses so shuffling shouldn't change that.
      assert(addrs.sorted(ZkMetadata.shardHashOrdering) ==
        Random.shuffle(addrs).sorted(ZkMetadata.shardHashOrdering))
    }
  }

  private val possibleIdenticalMetaData = Seq(
    None,
    Some(None),
    Some(Some(1)))

  test("addrs with the same metadata and ip are sorted by port") {
    possibleIdenticalMetaData.foreach { m =>
      assert(ZkMetadata.shardHashOrdering.compare(
        buildAddress("1.2.3.1", 1, m),
        buildAddress("1.2.3.1", 2, m)) < 0)

      assert(ZkMetadata.shardHashOrdering.compare(
        buildAddress("1.2.3.1", 2, m),
        buildAddress("1.2.3.1", 1, m)) > 0)
    }
  }

  test("addrs with different ip's are not identical") {
    possibleIdenticalMetaData.foreach { m =>
      assert(ZkMetadata.shardHashOrdering.compare(
        buildAddress("1.2.3.1", 1, m),
        buildAddress("1.2.3.2", 1, m)) != 0)
    }
  }
}
