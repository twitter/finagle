package com.twitter.finagle.thrift

import com.twitter.finagle._
import com.twitter.finagle.service.ResponseClassifier
import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.thrift.service.Filterable
import org.apache.thrift.protocol.TProtocolFactory

/**
 * Typeclass ServiceIfaceBuilder[T] creates T-typed interfaces from thrift clients.
 * Scrooge generates implementations of this builder.
 */
@deprecated("Use com.twitter.finagle.thrift.service.ServicePerEndpointBuilder", "2017-11-13")
trait ServiceIfaceBuilder[ServiceIface <: Filterable[ServiceIface]] {

  /**
   * Build a client ServiceIface wrapping a binary thrift service.
   *
   * @param thriftService An underlying thrift service that works on byte arrays.
   * @param clientParam RichClientParam wraps client params [[com.twitter.finagle.thrift.RichClientParam]].
   */
  def newServiceIface(
    thriftService: Service[ThriftClientRequest, Array[Byte]],
    clientParam: RichClientParam
  ): ServiceIface

  @deprecated("Use com.twitter.finagle.thrift.RichClientParam", "2017-08-16")
  def newServiceIface(
    thriftService: Service[ThriftClientRequest, Array[Byte]],
    pf: TProtocolFactory = Protocols.binaryFactory(),
    stats: StatsReceiver = NullStatsReceiver,
    responseClassifier: ResponseClassifier = ResponseClassifier.Default
  ): ServiceIface = {
    val clientParam =
      RichClientParam(pf, clientStats = stats, responseClassifier = responseClassifier)
    newServiceIface(thriftService, clientParam)
  }

  @deprecated("Use com.twitter.finagle.thrift.RichClientParam", "2017-08-16")
  def newServiceIface(
    thriftService: Service[ThriftClientRequest, Array[Byte]],
    pf: TProtocolFactory,
    stats: StatsReceiver
  ): ServiceIface = {
    val clientParam = RichClientParam(pf, clientStats = stats)
    newServiceIface(thriftService, clientParam)
  }
}

/**
 * A typeclass to construct a MethodIface by wrapping a ServiceIface.
 * This is a compatibility constructor to replace an existing Future interface
 * with one built from a ServiceIface.
 *
 * Scrooge generates implementations of this builder.
 */
@deprecated("Use com.twitter.finagle.thrift.service.MethodPerEndpointBuilder", "2017-11-13")
trait MethodIfaceBuilder[ServiceIface, MethodIface] {

  /**
   * Build a FutureIface wrapping a ServiceIface.
   */
  def newMethodIface(serviceIface: ServiceIface): MethodIface
}
