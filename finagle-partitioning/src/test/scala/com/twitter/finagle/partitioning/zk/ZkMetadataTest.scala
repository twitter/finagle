package com.twitter.finagle.partitioning.zk

import com.twitter.finagle.{Addr, Address}
import java.net.InetSocketAddress
import org.scalacheck.Gen
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import scala.util.Random
import org.scalatest.funsuite.AnyFunSuite

class ZkMetadataTest extends AnyFunSuite with ScalaCheckDrivenPropertyChecks {
  val shardHashOrdering = ZkMetadata.shardHashOrdering(ZkMetadata.key.hashCode)
  val metadata = ZkMetadata(Some(4))

  test("toAddrMetadata") {
    val addrMetadata = ZkMetadata.toAddrMetadata(metadata)
    assert(addrMetadata(ZkMetadata.key) == metadata)
  }

  test("ZkMetadata construction throws on large metadata size") {
    val metadata = (for (key <- 0 to 11; value <- 0 to 11)
      yield key.toString -> value.toString).toMap

    intercept[AssertionError] {
      ZkMetadata(Some(4), metadata)
    }
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
    def sort(addrs: Seq[Address]): Seq[Address] = addrs.sorted(shardHashOrdering)
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

    assert(heterogenous.sorted(shardHashOrdering) != heterogenous)
  }

  private def buildAddress(
    address: String,
    port: Int,
    shardMetaData: Option[Option[Int]]
  ): Address = {
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
      assert(shardHashOrdering.compare(a, noShardId) < 0)
      assert(shardHashOrdering.compare(noShardId, a) > 0)
    }

    // property based tests
    val addrGen = addressFromMetadata(Gen.oneOf(Gen.const(None), Gen.const(Some(None))))

    val withShardId = buildAddress("1.2.3.3", 3, Some(Some(1)))
    forAll(addrGen) { addr =>
      assert(shardHashOrdering.compare(withShardId, addr) < 0)
      assert(shardHashOrdering.compare(addr, withShardId) > 0)
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
      assert(shardHashOrdering.compare(a, base) < 0)
      assert(shardHashOrdering.compare(base, a) > 0)
    }

    // property based tests
    val withMetadata =
      addressFromMetadata(Gen.oneOf(Gen.const(Some(Some(1))), Gen.const(Some(None))))

    val withoutMetadata = addressFromMetadata(Gen.const(None))

    val toCompare = for {
      w <- withMetadata
      wo <- withoutMetadata
    } yield (w, wo)

    forAll(toCompare) {
      case (w, wo) =>
        assert(shardHashOrdering.compare(w, wo) < 0)
        assert(shardHashOrdering.compare(wo, w) > 0)
    }
  }

  test("transitivity: sorting properties for a ips") {
    val addrs = Seq(
      buildAddress("1.2.3.3", 0, Some(Some(2))),
      buildAddress("1.2.3.4", 0, Some(None)),
      buildAddress("1.2.3.4", 0, None),
      buildAddress("1.2.3.5", 0, Some(Some(2)))
    )

    assert(
      addrs.sorted(shardHashOrdering) ==
        Seq(
          buildAddress("1.2.3.5", 0, Some(Some(2))),
          buildAddress("1.2.3.3", 0, Some(Some(2))), // Second based on hash of ip
          buildAddress("1.2.3.4", 0, Some(None)),
          buildAddress("1.2.3.4", 0, None)
        )
    )
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
      assert(
        addrs.sorted(shardHashOrdering) ==
          Random.shuffle(addrs).sorted(shardHashOrdering)
      )
    }
  }

  private val possibleIdenticalMetaData = Seq(None, Some(None), Some(Some(1)))

  test("addrs with the same metadata and ip are sorted deterministically by port") {
    // Note that the ports are hashed, so the order may not be
    // logical. Rather, the important property we care about is
    // determinism.
    possibleIdenticalMetaData.foreach { m =>
      assert(
        shardHashOrdering.compare(buildAddress("1.2.3.1", 1, m), buildAddress("1.2.3.1", 2, m)) == 1
      )

      assert(
        shardHashOrdering
          .compare(buildAddress("1.2.3.1", 2, m), buildAddress("1.2.3.1", 1, m)) == -1
      )
    }
  }

  test("addrs with different ip's are not identical") {
    possibleIdenticalMetaData.foreach { m =>
      assert(
        shardHashOrdering.compare(buildAddress("1.2.3.1", 1, m), buildAddress("1.2.3.2", 1, m)) != 0
      )
    }
  }
}
