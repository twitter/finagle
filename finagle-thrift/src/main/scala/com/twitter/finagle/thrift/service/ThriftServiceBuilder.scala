package com.twitter.finagle.thrift.service

/**
 * A typeclass to construct a ThriftService by wrapping a ServicePerEndpoint.
 * This is a compatibility constructor to replace an existing Future interface
 * with one built from a ServicePerEndpoint.
 *
 * Scrooge generates implementations of this builder.
 */
@deprecated("Use MethodPerEndpointBuilder", "2018-01-12")
trait ThriftServiceBuilder[ServicePerEndpoint, ThriftService] {

  /**
   * Build a MethodPerEndpoint wrapping a ServicePerEndpoint.
   */
  def build(servicePerEndpoint: ServicePerEndpoint): ThriftService
}
