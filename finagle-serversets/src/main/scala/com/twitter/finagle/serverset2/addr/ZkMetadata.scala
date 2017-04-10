package com.twitter.finagle.serverset2.addr

import com.twitter.finagle.{Addr, Address}

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
   * Orders a Finagle [[Address]] based on its [[ZkMetadata]].
   */
  val AddressOrdering: Ordering[Address] = new Ordering[Address] {
    def compare(a0: Address, a1: Address): Int = (a0, a1) match {
      case (Address.Inet(_, md0), Address.Inet(_, md1)) =>
        (fromAddrMetadata(md0), fromAddrMetadata(md1)) match {
          case (Some(ZkMetadata(Some(id0))), Some(ZkMetadata(Some(id1)))) =>
            Integer.compare(id0, id1)
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
