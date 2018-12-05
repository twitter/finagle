package com.twitter.finagle.thrift

import com.twitter.finagle.thrift.service.{MethodPerEndpointBuilder, ThriftServiceBuilder}

/**
 * Stateless helper methods which wrap a given `ServiceIface` (deprecated) or a
 * given `ServicePerEndpoint` with another type via the given method's implicit Builder.
 */
trait ThriftClient {

  /**
   * Converts from a Service interface (`ServiceIface`) to the
   * method interface (`newIface`).
   */
  @deprecated(
    "Use com.twitter.finagle.ThriftClient#methodPerEndpoint[ServicePerEndpoint, MethodPerEndpoint]",
    "2017-11-13"
  )
  def newMethodIface[ServiceIface, FutureIface](
    serviceIface: ServiceIface
  )(
    implicit builder: MethodIfaceBuilder[ServiceIface, FutureIface]
  ): FutureIface = builder.newMethodIface(serviceIface)

  /**
   * Converts from a Service interface (`ServicePerEndpoint`) to the
   * method interface (`MethodPerEndpoint`).
   */
  def methodPerEndpoint[ServicePerEndpoint, MethodPerEndpoint](
    servicePerEndpoint: ServicePerEndpoint
  )(
    implicit builder: MethodPerEndpointBuilder[ServicePerEndpoint, MethodPerEndpoint]
  ): MethodPerEndpoint = builder.methodPerEndpoint(servicePerEndpoint)

  /**
   * Converts from a Service interface (`ServicePerEndpoint`) to the higher-kinded
   * method interface (`MethodPerEndpoint`).
   */
  @deprecated("Use methodPerEndpoint", "2018-01-12")
  def thriftService[ServicePerEndpoint, ThriftServiceType](
    servicePerEndpoint: ServicePerEndpoint
  )(
    implicit builder: ThriftServiceBuilder[ServicePerEndpoint, ThriftServiceType]
  ): ThriftServiceType = builder.build(servicePerEndpoint)
}
