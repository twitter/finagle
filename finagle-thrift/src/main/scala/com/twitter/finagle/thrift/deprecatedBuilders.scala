package com.twitter.finagle.thrift

import com.twitter.finagle._
import com.twitter.finagle.service.ResponseClassifier
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.thrift.service.Filterable
import org.apache.thrift.protocol.TProtocolFactory

/**
 * Typeclass ServiceIfaceBuilder[T] creates T-typed interfaces from thrift clients.
 * Scrooge generates implementations of this builder.
 */
@deprecated("Use com.twitter.finagle.thrift.service.ServicePerEndpointBuilder", "2017-11-13")
trait ServiceIfaceBuilder[ServiceIface <: Filterable[ServiceIface]] {

  /**
   * A runtime class for this service. This is known at compile time and is filled in by Scrooge.
   */
  def serviceClass: Class[ServiceIface] = {
    // This is a temporary hack until we figure out how to restore a proper Scrooge bootstrapping.
    // Adding a new method with implementation to a trait should be both source & binary compatible
    // change, which would allow IDL classes generated with an older Scrooge to compile & run
    // against newer Finagle.
    null
  }

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
