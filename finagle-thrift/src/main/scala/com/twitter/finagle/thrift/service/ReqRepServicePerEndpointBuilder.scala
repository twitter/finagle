package com.twitter.finagle.thrift.service

/**
 * Typeclass ReqRepServicePerEndpointBuilder[T] creates T-typed interfaces from thrift clients.
 *
 * Scrooge generates implementations of this builder.
 */
trait ReqRepServicePerEndpointBuilder[
  ReqRepServicePerEndpoint <: Filterable[
    ReqRepServicePerEndpoint
  ]] extends ServicePerEndpointBuilder[ReqRepServicePerEndpoint]
