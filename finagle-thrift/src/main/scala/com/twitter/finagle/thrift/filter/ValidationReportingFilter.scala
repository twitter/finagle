package com.twitter.finagle.thrift.filter

import com.twitter.finagle.Service
import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.SimpleFilter
import com.twitter.finagle.Stack
import com.twitter.finagle.Stackable
import com.twitter.finagle.param
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.thrift.filter.ValidationReportingFilter.log
import com.twitter.logging.Logger
import com.twitter.scrooge.thrift_validation.ThriftValidationException
import com.twitter.util.Future
import com.twitter.util.Throw

object ValidationReportingFilter {

  private val log = Logger.get(classOf[ValidationReportingFilter[_, _]])

  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module1[param.Stats, ServiceFactory[Req, Rep]] {
      val role: Stack.Role = Stack.Role("ValidationReportingFilter")
      val description: String = "Report stats on requests that fail Thrift validation"

      def make(
        statsParam: param.Stats,
        next: ServiceFactory[Req, Rep]
      ): ServiceFactory[Req, Rep] = {
        if (statsParam.statsReceiver.isNull) next
        else {
          new ValidationReportingFilter[Req, Rep](
            statsParam.statsReceiver.scope("thrift_validation"))
            .andThen(next)
        }
      }
    }
}

/**
 * Introduce a new ReportingFilter to inspect any `ThriftValidationException` thrown from the
 * Finagle Stack. To report metrics, the reporting filter will carry a StatsReceiver, which
 * reports the number of invalid requests through a counter.
 *
 */
class ValidationReportingFilter[Req, Rep](
  statsReceiver: StatsReceiver)
    extends SimpleFilter[Req, Rep] {

  def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
    service(request).respond {
      case Throw(e: ThriftValidationException) =>
        // the counter reports the number of invalid requests meaning requests with violations
        // that throw exception. we report the endpoint and the class name for the exception
        statsReceiver.counter("violation", e.endpoint, e.requestClazz.getName).incr()
        log.info(e, "Discovered violations in the request")
      case _ => () // do nothing
    }
  }
}
