package com.twitter.finagle.serverset2.addr

import com.twitter.finagle.{Addr, Address}
import scala.util.hashing.MurmurHash3

/**
 * Zookeeper per-address metadata.
 *
 * Note that weight also comes from Zookeeper, but it is consumed
 * directly by Finagle's load balancer so it is represented separately
 * as [[com.twitter.finagle.addr.WeightedAddress]]. Weights are treated
 * specially since their interpretation requires deep integration into
 * the Finagle client stack.
 *
 * @param shardId The shard ID of the address.
 */
case class ZkMetadata(shardId: Option[Int])

object ZkMetadata {
  // visibility exposed for testing
  private[addr] val key = "zk_metadata"

  /**
   * Orders a Finagle [[Address]] based on a deterministic hash of its shard id.
   */
  val shardHashOrdering: Ordering[Address] = new Ordering[Address] {
    private[this] val hashSeed = key.hashCode

    private[this] def hash(shardId: Int): Int = {
      // We effectively unroll what MurmurHash3.bytesHash does for a single
      // iteration. That is, it packs groups of four bytes into an int and
      // mixes then finalizes.
      MurmurHash3.finalizeHash(
        hash = MurmurHash3.mixLast(hashSeed, shardId),
        // we have four bytes in `shardId`
        length = 4
      )
    }

    def compare(a0: Address, a1: Address): Int = (a0, a1) match {
      case (Address.Inet(_, md0), Address.Inet(_, md1)) =>
        (fromAddrMetadata(md0), fromAddrMetadata(md1)) match {
          case (Some(ZkMetadata(Some(id0))), Some(ZkMetadata(Some(id1)))) =>
            Integer.compare(hash(id0), hash(id1))
          // If they don't have two shardIds to compare, we don't really care
          // about the ordering only that it's consistent.
          case (Some(ZkMetadata(Some(_))), Some(ZkMetadata(None))) => -1
          case (Some(ZkMetadata(None)), Some(ZkMetadata(Some(_)))) => 1
          case (Some(ZkMetadata(None)), Some(ZkMetadata(None))) => 0
          case (Some(_), None) => -1
          case (None, Some(_)) => 1
          case (None, None) => 0
        }
      case _ => 0
    }

    override def toString: String = "ZkMetadata.shardHashOrdering"
  }

  /**
   * Convert [[ZkMetadata]] to an instance of
   * [[com.twitter.finagle.Addr.Metadata]].
   */
  def toAddrMetadata(metadata: ZkMetadata): Addr.Metadata =
    Addr.Metadata(key -> metadata)

  /**
   * Convert [[com.twitter.finagle.Addr.Metadata]] to an instance of
   * [[ZkMetadata]].  If [[ZkMetadata]] is not present in `metadata`,
   * return None.
   */
  def fromAddrMetadata(metadata: Addr.Metadata): Option[ZkMetadata] =
    metadata.get(key) match {
      case Some(metadata: ZkMetadata) => Some(metadata)
      case _ => None
    }
}
