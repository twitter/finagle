package com.twitter.finagle.thrift.service

/**
 * A typeclass to construct a MethodPerEndpoint by wrapping a ReqRepServicePerEndpoint.
 *
 * This is a compatibility constructor to replace an existing Future interface with one built from
 * a ReqRepServicePerEndpoint.
 *
 * Scrooge generates implementations of this builder.
 */
trait ReqRepMethodPerEndpointBuilder[ReqRepServicePerEndpoint, MethodPerEndpoint]
    extends MethodPerEndpointBuilder[ReqRepServicePerEndpoint, MethodPerEndpoint]
