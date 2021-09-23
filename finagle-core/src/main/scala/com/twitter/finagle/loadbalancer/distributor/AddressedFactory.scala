package com.twitter.finagle.loadbalancer.distributor

import com.twitter.finagle.Address
import com.twitter.finagle.addr.WeightedAddress
import com.twitter.finagle.loadbalancer.EndpointFactory

/**
 * A [[ServiceFactory]] and its associated [[Address]].
 */
case class AddressedFactory[Req, Rep](factory: EndpointFactory[Req, Rep], address: Address) {
  lazy val weight: Double = WeightedAddress.extract(address)._2
}
