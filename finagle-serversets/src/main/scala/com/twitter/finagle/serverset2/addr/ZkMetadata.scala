package com.twitter.finagle.serverset2.addr

import com.twitter.finagle.Addr

/**
 * Zookeeper per-address metadata.
 *
 * Note that weight also comes from Zookeeper, but it is consumed
 * directly by Finagle's load balancer so it is represented separately
 * as [[com.twitter.finagle.addr.WeightedAddress]]. Ideally, the
 * handling of weight should not exist in finagle-core.
 *
 * @param shardId The shard ID of the address.
 */
case class ZkMetadata(shardId: Option[Int])

object ZkMetadata {
  // visibility exposed for testing
  private[addr] val key = "zk_metadata"
  private val default = ZkMetadata(None)

  /**
   * Convert [[ZkMetadata]] to an instance of
   * [[com.twitter.finagle.Addr.Metadata]].
   */
  def toAddrMetadata(metadata: ZkMetadata): Addr.Metadata =
    Addr.Metadata(key -> metadata)

  /**
   * Convert [[com.twitter.finagle.Addr.Metadata]] to an instance of
   * [[ZkMetadata]].  If [[ZkMetadata]] is not present in `metadata`,
   * return the default metadata.
   */
  def fromAddrMetadata(metadata: Addr.Metadata): ZkMetadata =
    metadata.get(key) match {
      case Some(metadata: ZkMetadata) => metadata
      case _ => default
    }

  /**
   * Pattern match against `metadata` for [[ZkMetadata]] and return the
   * shard ID if it exists.
   */
  def unapply(metadata: Addr.Metadata): Option[Int] =
    fromAddrMetadata(metadata).shardId
}
