package com.twitter.finagle.addr

import com.twitter.finagle.Addr
import com.twitter.finagle.Address
import com.twitter.finagle.ServiceFactory

/**
 * This object contains utility functions for adding and removing
 * weight metadata for [[com.twitter.finagle.Address]].
 *
 * These weights are consumed by a
 * [[com.twitter.finagle.factory.TrafficDistributor]].
 */
object WeightedAddress {
  val weightKey = "endpoint_addr_weight"
  private val defaultWeight: Double = 1.0

  /**
   * Returns a new [[com.twitter.finagle.Address]] with attached
   * weight metadata. If the weight entry already exists in the metadata,
   * it is overwritten.
   */
  def apply(addr: Address, weight: Double): Address =
    addr match {
      case Address.Inet(ia, metadata) =>
        Address.Inet(ia, metadata + (weightKey -> weight))
      case Address.ServiceFactory(sf: ServiceFactory[_, _], metadata) =>
        Address.ServiceFactory(sf, metadata + (weightKey -> weight))
      case addr => addr
    }

  /**
   * A variant of `extract` that pattern matches against weight metadata,
   * returning the unweighted address and weight. Note that the input
   * will always match, even if weight metadata is not present (in which
   * case it returns the default weight 1.0).
   */
  def unapply(addr: Address): Option[(Address, Double)] =
    Some(extract(addr))

  /**
   * Extracts weight from metadata and returns an unweighted copy of
   * `addr`. Returns the default value (1.0) if weight entry does not exist.
   */
  def extract(addr: Address): (Address, Double) =
    addr match {
      case Address.Inet(ia, metadata) =>
        (Address.Inet(ia, metadata - weightKey), weight(metadata))
      case Address.ServiceFactory(sf: ServiceFactory[_, _], metadata) =>
        (Address.ServiceFactory(sf, metadata - weightKey), weight(metadata))
      case addr =>
        (addr, defaultWeight)
    }

  private def weight(metadata: Addr.Metadata): Double =
    metadata.get(weightKey) match {
      case Some(weight: Double) => weight
      case _ => defaultWeight
    }
}
