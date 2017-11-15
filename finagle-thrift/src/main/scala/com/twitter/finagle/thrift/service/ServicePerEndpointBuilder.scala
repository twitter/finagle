package com.twitter.finagle.thrift.service

import com.twitter.finagle.Service
import com.twitter.finagle.thrift.{RichClientParam, ThriftClientRequest, ThriftServiceIface}

/**
 * Typeclass ServicePerEndpointBuilder[T] creates T-typed interfaces from thrift clients.
 * Scrooge generates implementations of this builder.
 */
trait ServicePerEndpointBuilder[ServicePerEndpoint <: ThriftServiceIface.Filterable[ServicePerEndpoint]] {

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
