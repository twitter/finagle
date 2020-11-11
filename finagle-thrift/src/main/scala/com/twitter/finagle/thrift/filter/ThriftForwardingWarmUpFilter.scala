package com.twitter.finagle.thrift.filter

import com.twitter.finagle.filter.ForwardingWarmUpFilter
import com.twitter.finagle.stats.{DefaultStatsReceiver, StatsReceiver}
import com.twitter.finagle.thrift.{ClientId, ThriftClientRequest}
import com.twitter.finagle.{Filter, Service}
import com.twitter.util.Duration

object ThriftForwardingWarmUpFilter {
  val thriftForwardingWarmupFilter =
    new Filter[Array[Byte], Array[Byte], ThriftClientRequest, Array[Byte]] {
      override def apply(request: Array[Byte], service: Service[ThriftClientRequest, Array[Byte]]) =
        service(new ThriftClientRequest(request, false))
    }
}

import ThriftForwardingWarmUpFilter.thriftForwardingWarmupFilter

class ThriftForwardingWarmUpFilter(
  warmupPeriod: Duration,
  forwardTo: Service[ThriftClientRequest, Array[Byte]],
  statsReceiver: StatsReceiver = DefaultStatsReceiver,
  isBypassClient: ClientId => Boolean = _ => true)
    extends ForwardingWarmUpFilter[Array[Byte], Array[Byte]](
      warmupPeriod,
      thriftForwardingWarmupFilter andThen forwardTo,
      statsReceiver
    ) {

  override def bypassForward: Boolean = ClientId.current.forall(isBypassClient)
}
