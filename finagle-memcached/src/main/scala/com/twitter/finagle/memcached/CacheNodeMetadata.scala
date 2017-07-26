package com.twitter.finagle.memcached

import com.twitter.finagle.Addr

/**
 * Cache node metadata consumed by clients that implement sharding (see
 * [[com.twitter.finagle.memcached.KetamaPartitionedClient]]).
 *
 * This class and its companion object are private because they are only an implementation detail for
 * converting between [[com.twitter.finagle.memcached.CacheNode]] and
 * [[com.twitter.finagle.Address]]. We need to convert between these types for backwards
 * compatibility: [[com.twitter.finagle.memcached.KetamaPartitionedClient]] consumes
 * [[com.twitter.finagle.memcached.CacheNode]]s but [[com.twitter.finagle.Resolver]]s return
 * [[com.twitter.finagle.Address]]s.
 *
 * @param weight The weight of the cache node. Default value is 1. Note that this determines where
 * data is stored in the Ketama ring and is not interchangable with the notion of weight in
 * [[com.twitter.finagle.addr.WeightedAddress]], which pertains to load balancing.
 * @param key An optional unique identifier for the cache node (e.g.  shard ID).
 */
private[memcached] case class CacheNodeMetadata(weight: Int, key: Option[String])

private[memcached] object CacheNodeMetadata {
  private val key = "cache_node_metadata"

  /**
   * Convert [[CacheNodeMetadata]] to an instance of
   * [[com.twitter.finagle.Addr.Metadata]].
   */
  def toAddrMetadata(metadata: CacheNodeMetadata): Addr.Metadata =
    Addr.Metadata(key -> metadata)

  /**
   * Convert [[com.twitter.finagle.Addr.Metadata]] to an instance of
   * [[CacheNodeMetadata]].
   */
  def fromAddrMetadata(metadata: Addr.Metadata): Option[CacheNodeMetadata] =
    metadata.get(key) match {
      case some @ Some(metadata: CacheNodeMetadata) =>
        some.asInstanceOf[Option[CacheNodeMetadata]]
      case _ => None
    }

  /**
   * A variant of `fromAddrMetadata` that pattern matches against weight
   * and key of the [[CacheNodeMetadata]].
   */
  def unapply(metadata: Addr.Metadata): Option[(Int, Option[String])] =
    fromAddrMetadata(metadata).map {
      case CacheNodeMetadata(weight, key) =>
        (weight, key)
    }
}
