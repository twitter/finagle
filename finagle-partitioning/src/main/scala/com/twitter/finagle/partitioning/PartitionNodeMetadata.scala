package com.twitter.finagle.partitioning

import com.twitter.finagle.Addr

/**
 * Cache node metadata consumed by clients that implement sharding (see
 * [[com.twitter.finagle.partitioning.ConsistentHashPartitioningService]]).
 *
 * This class and its companion object are private because they are only an implementation detail for
 * converting between [[com.twitter.finagle.partitioning.PartitionNode]] and
 * [[com.twitter.finagle.Address]]. We need to convert between these types for backwards
 * compatibility: [[com.twitter.finagle.partitioning.ConsistentHashPartitioningService]] consumes
 * [[PartitionNode]]s but [[com.twitter.finagle.Resolver]]s return
 * [[com.twitter.finagle.Address]]s.
 *
 * @param weight The weight of the cache node. Default value is 1. Note that this determines where
 * data is stored in the hash ring and is not interchangeable with the notion of weight in
 * [[com.twitter.finagle.addr.WeightedAddress]], which pertains to load balancing.
 * @param key An optional unique identifier for the cache node (e.g.  shard ID).
 */
private[finagle] case class PartitionNodeMetadata(weight: Int, key: Option[String])

private[finagle] object PartitionNodeMetadata {
  private val key = "cache_node_metadata"

  /**
   * Convert [[PartitionNodeMetadata]] to an instance of
   * [[com.twitter.finagle.Addr.Metadata]].
   */
  def toAddrMetadata(metadata: PartitionNodeMetadata): Addr.Metadata =
    Addr.Metadata(key -> metadata)

  /**
   * Convert [[com.twitter.finagle.Addr.Metadata]] to an instance of
   * [[PartitionNodeMetadata]].
   */
  def fromAddrMetadata(metadata: Addr.Metadata): Option[PartitionNodeMetadata] =
    metadata.get(key) match {
      case some @ Some(metadata: PartitionNodeMetadata) =>
        some.asInstanceOf[Option[PartitionNodeMetadata]]
      case _ => None
    }

  /**
   * A variant of `fromAddrMetadata` that pattern matches against weight
   * and key of the [[PartitionNodeMetadata]].
   */
  def unapply(metadata: Addr.Metadata): Option[(Int, Option[String])] =
    fromAddrMetadata(metadata).map {
      case PartitionNodeMetadata(weight, key) =>
        (weight, key)
    }
}
