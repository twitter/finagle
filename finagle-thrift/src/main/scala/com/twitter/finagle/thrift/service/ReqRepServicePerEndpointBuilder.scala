package com.twitter.finagle.thrift.service

import com.twitter.finagle.Service
import com.twitter.finagle.thrift.{RichClientParam, ThriftClientRequest}

/**
 * Typeclass ReqRepServicePerEndpointBuilder[T] creates T-typed interfaces from thrift clients.
 *
 * Scrooge generates implementations of this builder.
 */
trait ReqRepServicePerEndpointBuilder[ReqRepServicePerEndpoint <: Filterable[ReqRepServicePerEndpoint]] {

  /**
   * Build a client ReqRepServicePerEndpoint wrapping a binary thrift service.
   *
   * @param thriftService An underlying thrift service that works on byte arrays.
   * @param clientParam RichClientParam wraps client params [[com.twitter.finagle.thrift.RichClientParam]].
   */
  def servicePerEndpoint(
    thriftService: Service[ThriftClientRequest, Array[Byte]],
    clientParam: RichClientParam
  ): ReqRepServicePerEndpoint
}
