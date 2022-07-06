package com.twitter.finagle.thrift.service

import com.twitter.finagle.Service
import com.twitter.finagle.thrift.RichClientParam
import com.twitter.finagle.thrift.ThriftClientRequest

/**
 * Typeclass ServicePerEndpointBuilder[T] creates T-typed interfaces from thrift clients.
 * Scrooge generates implementations of this builder.
 */
trait ServicePerEndpointBuilder[
  ServicePerEndpoint <: Filterable[
    ServicePerEndpoint
  ]] {

  /**
   * A runtime class for this service. This is known at compile time and is filled in by Scrooge.
   */
  def serviceClass: Class[ServicePerEndpoint] = {
    // This is a temporary hack until we figure out how to restore a proper Scrooge
    // bootstrapping. Adding a new method with implementation to a trait should be both source &
    // binary compatible change, which would allow IDL classes generated with an older Scrooge to
    // compile & run against newer Finagle.
    null
  }

  /**
   * Build a client ServicePerEndpoint wrapping a binary thrift service.
   *
   * @param thriftService An underlying thrift service that works on byte arrays.
   * @param clientParam RichClientParam wraps client params [[com.twitter.finagle.thrift.RichClientParam]].
   */
  def servicePerEndpoint(
    thriftService: Service[ThriftClientRequest, Array[Byte]],
    clientParam: RichClientParam
  ): ServicePerEndpoint

}
