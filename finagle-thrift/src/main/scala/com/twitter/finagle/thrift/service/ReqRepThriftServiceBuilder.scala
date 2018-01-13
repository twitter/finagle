package com.twitter.finagle.thrift.service

/**
 * A typeclass to construct a ThriftService by wrapping a ReqRepServicePerEndpoint.
 *
 * This is a compatibility constructor to replace an existing Future interface with one built from
 * a ReqRepServicePerEndpoint.
 *
 * Scrooge generates implementations of this builder.
 */
@deprecated("Use MethodPerEndpointBuilder", "2018-01-12")
trait ReqRepThriftServiceBuilder[ReqRepServicePerEndpoint, ThriftService]
  extends ThriftServiceBuilder[ReqRepServicePerEndpoint, ThriftService]
