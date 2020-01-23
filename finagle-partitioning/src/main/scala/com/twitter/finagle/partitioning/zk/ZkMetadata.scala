package com.twitter.finagle.partitioning.zk

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
 * @param metadata A generic metadata container associated with the address.
 */
case class ZkMetadata(shardId: Option[Int], metadata: Map[String, String] = Map.empty) {
  assert(
    metadata.size <= ZkMetadata.maxMetadataEntries,
    s"Given size of metadata map: ${metadata.size} is greater than ${ZkMetadata.maxMetadataEntries}")
}

object ZkMetadata {
  // visibility exposed for testing
  private[partitioning] val key = "zk_metadata"

  // The maximum size of the [[ZkMetadata.metadata]] map that is allowed.
  // This bounds the metadata map's size that is allowed to flow through
  // the service discovery stack.
  private val maxMetadataEntries = 10

  /**
   * Orders a Finagle [[Address]] based on a deterministic hash of its shard id.
   *
   * If shard id information is identical, either both lack metadata, both lack
   * a shard id, or both have the same shard id, ordering is then computed by
   * [[Address.hashOrdering]].
   *
   * @note Avoiding collisions in this hash ordering where inputs are equal is
   * an important property in keeping it deterministic. Thus, it uses a murmurhash
   * under the hood which is known to not have collisions for 32-bit inputs. However,
   * if the input collection does not have shard ids available, we fall back to
   * [[Address.hashOrdering]] which may have some caveats to this.
   *
   * @note We do not consider [[ZkMetadata.metadata]] while comparing the [[Address]]
   * and only [[ZkMetadata.shardId]] portion of the [[ZkMetadata]] is considered.
   */
  def shardHashOrdering(seed: Int): Ordering[Address] = new Ordering[Address] {
    private[this] def hash(shardId: Int): Int = {
      // We effectively unroll what MurmurHash3.bytesHash does for a single
      // iteration. That is, it packs groups of four bytes into an int and
      // mixes then finalizes.
      MurmurHash3.finalizeHash(
        hash = MurmurHash3.mixLast(seed, shardId),
        // we have four bytes in `shardId`
        length = 4
      )
    }

    private[this] val addressHashOrder = Address.hashOrdering(seed)

    def compare(a0: Address, a1: Address): Int = (a0, a1) match {
      case (Address.Inet(_, md0), Address.Inet(_, md1)) =>
        (fromAddrMetadata(md0), fromAddrMetadata(md1)) match {
          case (Some(ZkMetadata(Some(id0), _)), Some(ZkMetadata(Some(id1), _))) =>
            if (id0 == id1) addressHashOrder.compare(a0, a1)
            else Integer.compare(hash(id0), hash(id1))
          case (Some(ZkMetadata(Some(_), _)), Some(ZkMetadata(None, _))) => -1
          case (Some(ZkMetadata(None, _)), Some(ZkMetadata(Some(_), _))) => 1
          case (Some(ZkMetadata(None, _)), Some(ZkMetadata(None, _))) =>
            addressHashOrder.compare(a0, a1)
          case (Some(_), None) => -1
          case (None, Some(_)) => 1
          case (None, None) =>
            addressHashOrder.compare(a0, a1)
        }
      case _ =>
        addressHashOrder.compare(a0, a1)
    }

    override def toString: String = s"ZkMetadata.shardHashOrdering($seed)"
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
