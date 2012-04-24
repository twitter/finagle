package com.twitter.finagle.thrift

import com.twitter.finagle.{RequestException, SimpleFilter, Service}
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver}
import com.twitter.util.Future

/**
 * This filter can only be used with finagle thrift codec.
 */
class ClientIdRequiredFilter[Req, Rep](statsReceiver: StatsReceiver = NullStatsReceiver)
  extends SimpleFilter[Req, Rep]
{
  private[this] val noClientIdSpecifiedEx = new NoClientIdSpecifiedException
  private[this] val filterCounter = statsReceiver.counter("no_client_id_specified")

  def apply(req: Req, service: Service[Req, Rep]) = {
    ClientId.current match {
      case Some(_) => service(req)
      case None =>
        filterCounter.incr()
        Future.exception(noClientIdSpecifiedEx)
    }
  }
}

class NoClientIdSpecifiedException extends RequestException

