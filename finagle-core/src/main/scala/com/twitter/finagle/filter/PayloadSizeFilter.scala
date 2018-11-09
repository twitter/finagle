package com.twitter.finagle.filter

import com.twitter.finagle.Stack.{Module1, Role}
import com.twitter.finagle._
import com.twitter.finagle.param.Stats
import com.twitter.finagle.stats.{StatsReceiver, Verbosity}
import com.twitter.finagle.tracing.{Trace, Tracing}
import com.twitter.util.{Future, Return, Try}

/**
 * A filter that exports two histograms to a given [[StatsReceiver]].
 *
 * 1. "request_payload_bytes" - a distribution of request payload sizes in bytes
 * 2. "response_payload_bytes" - a distribution of response payload sizes in bytes
 */
private[finagle] class PayloadSizeFilter[Req, Rep](
  statsReceiver: StatsReceiver,
  reqTraceKey: String,
  repTraceKey: String,
  reqSize: Req => Int,
  repSize: Rep => Int
) extends SimpleFilter[Req, Rep] {

  private[this] val requestBytes = statsReceiver.stat(Verbosity.Debug, "request_payload_bytes")
  private[this] val responseBytes = statsReceiver.stat(Verbosity.Debug, "response_payload_bytes")

  private[this] def recordRepSize(trace: Tracing): Try[Rep] => Unit = {
    case Return(rep) =>
      val size = repSize(rep)
      trace.recordBinary(repTraceKey, size.toString)
      responseBytes.add(size.toFloat)
    case _ =>
  }

  def apply(req: Req, service: Service[Req, Rep]): Future[Rep] = {
    val size = reqSize(req)
    val trace = Trace()
    trace.recordBinary(reqTraceKey, size.toString)
    requestBytes.add(size.toFloat)
    service(req).respond(recordRepSize(trace))
  }
}

private[finagle] object PayloadSizeFilter {

  val Role: Stack.Role = Stack.Role("PayloadSize")
  val Description: String = "Reports request/response payload sizes"

  private def module[Req, Rep](
    reqTraceKey: String,
    repTraceKey: String,
    reqSize: Req => Int,
    repSize: Rep => Int
  ): Stackable[ServiceFactory[Req, Rep]] = new Module1[param.Stats, ServiceFactory[Req, Rep]] {
    override def role: Role = PayloadSizeFilter.Role
    override def description: String = PayloadSizeFilter.Description

    override def make(stats: Stats, next: ServiceFactory[Req, Rep]): ServiceFactory[Req, Rep] = {
      val filter =
        new PayloadSizeFilter(stats.statsReceiver, reqTraceKey, repTraceKey, reqSize, repSize)
      filter.andThen(next)
    }
  }

  val ClientReqTraceKey: String = "clnt/request_payload_bytes"
  val ClientRepTraceKey: String = "clnt/response_payload_bytes"

  def clientModule[Req, Rep](
    reqSize: Req => Int,
    repSize: Rep => Int
  ): Stackable[ServiceFactory[Req, Rep]] = module(
    ClientReqTraceKey,
    ClientRepTraceKey,
    reqSize,
    repSize
  )

  val ServerReqTraceKey: String = "srv/request_payload_bytes"
  val ServerRepTraceKey: String = "srv/response_payload_bytes"

  def serverModule[Req, Rep](
    reqSize: Req => Int,
    repSize: Rep => Int
  ): Stackable[ServiceFactory[Req, Rep]] = module(
    ServerReqTraceKey,
    ServerRepTraceKey,
    reqSize,
    repSize
  )
}
