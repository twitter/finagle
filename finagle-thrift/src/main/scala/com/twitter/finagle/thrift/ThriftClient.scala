package com.twitter.finagle.thrift

import com.twitter.finagle.thrift.service.MethodPerEndpointBuilder

/**
 * Stateless helper methods which wrap a given `ServiceIface` (deprecated) or a
 * given `ServicePerEndpoint` with another type via the given method's implicit Builder.
 */
trait ThriftClient {

  /**
   * Converts from a Service interface (`ServicePerEndpoint`) to the
   * method interface (`MethodPerEndpoint`).
   */
  def methodPerEndpoint[ServicePerEndpoint, MethodPerEndpoint](
    servicePerEndpoint: ServicePerEndpoint
  )(
    implicit builder: MethodPerEndpointBuilder[ServicePerEndpoint, MethodPerEndpoint]
  ): MethodPerEndpoint = builder.methodPerEndpoint(servicePerEndpoint)
}
