package com.twitter.finagle.thrift

import com.twitter.finagle.{RequestException, SimpleFilter, Service}
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver}
import com.twitter.util.Future

/**
 * Indicates that a request without a [[com.twitter.finagle.thrift.ClientId]]
 * was issued to a server that requires them. See
 * [[com.twitter.finagle.thrift.ClientIdRequiredFilter]] for details.
 */
class NoClientIdSpecifiedException extends RequestException

/**
 * A [[com.twitter.finagle.Filter]] for Thrift services that enforces all
 * requests specify a [[com.twitter.finagle.thrift.ClientId]].
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
