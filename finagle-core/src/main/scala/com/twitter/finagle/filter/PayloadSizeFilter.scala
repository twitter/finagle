package com.twitter.finagle.filter

import com.twitter.finagle.Stack.{Module1, Role}
import com.twitter.finagle.param.Stats
import com.twitter.finagle.stats.{StatsReceiver, Verbosity}
import com.twitter.finagle._
import com.twitter.util.{Future, Return, Try}

/**
 * A filter that exports two histograms to a given [[StatsReceiver]].
 *
 * 1. "request_payload_bytes" - a distribution of request payload sizes in bytes
 * 2. "response_payload_bytes" - a distribution of response payload sizes in bytes
 */
private[finagle] class PayloadSizeFilter[Req, Rep](
  statsReceiver: StatsReceiver,
  reqSize: Req => Int,
  repSize: Rep => Int
) extends SimpleFilter[Req, Rep] {

  private[this] val requestBytes = statsReceiver.stat(Verbosity.Debug, "request_payload_bytes")
  private[this] val responseBytes = statsReceiver.stat(Verbosity.Debug, "response_payload_bytes")

  private[this] val recordRepSize: Try[Rep] => Unit = {
    case Return(rep) =>
      responseBytes.add(repSize(rep).toFloat)
    case _ =>
  }

  def apply(req: Req, service: Service[Req, Rep]): Future[Rep] = {
    requestBytes.add(reqSize(req).toFloat)
    service(req).respond(recordRepSize)
  }
}

private[finagle] object PayloadSizeFilter {

  val Role: Stack.Role = Stack.Role("PayloadSize")
  val Description: String = "Reports request/response payload sizes"

  def module[Req, Rep](
    reqSize: Req => Int,
    repSize: Rep => Int
  ): Stackable[ServiceFactory[Req, Rep]] = new Module1[param.Stats, ServiceFactory[Req, Rep]] {
    override def role: Role = PayloadSizeFilter.Role
    override def description: String = PayloadSizeFilter.Description

    override def make(stats: Stats, next: ServiceFactory[Req, Rep]): ServiceFactory[Req, Rep] =
      new PayloadSizeFilter(stats.statsReceiver, reqSize, repSize).andThen(next)
  }
}
