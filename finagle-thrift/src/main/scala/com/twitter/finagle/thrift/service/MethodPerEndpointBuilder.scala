package com.twitter.finagle.thrift.service

/**
 * A typeclass to construct a MethodPerEndpoint by wrapping a ServicePerEndpoint.
 * This is a compatibility constructor to replace an existing Future interface
 * with one built from a ServicePerEndpoint.
 *
 * Scrooge generates implementations of this builder.
 */
trait MethodPerEndpointBuilder[ServicePerEndpoint, MethodPerEndpoint] {

  /**
   * Build a MethodPerEndpoint wrapping a ServicePerEndpoint.
   */
  def methodPerEndpoint(servicePerEndpoint: ServicePerEndpoint): MethodPerEndpoint

}
