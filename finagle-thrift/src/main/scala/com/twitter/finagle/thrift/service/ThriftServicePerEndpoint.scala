package com.twitter.finagle.thrift.service

import com.twitter.finagle.{Filter, Service, SimpleFilter}
import com.twitter.finagle.service.ResponseClassifier
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.thrift.{RichClientParam, ThriftClientRequest, ThriftMethodStats}
import com.twitter.scrooge.ThriftMethod
import com.twitter.util.Future
import org.apache.thrift.TApplicationException
import org.apache.thrift.protocol.TProtocolFactory

/**
 * Construct Service interface for a Thrift method.
 *
 * There are two ways to use a Scrooge-generated Thrift `Service` with Finagle:
 *
 * 1. Using a Service interface, i.e. a collection of Finagle `Services`.
 *
 * 2. Using a method interface, i.e. a collection of methods returning `Futures`.
 *
 * Example: for a Thrift service IDL:
 * {{{
 * service Logger {
 *   string log(1: string message, 2: i32 logLevel);
 *   i32 getLogSize();
 * }
 * }}}
 *
 * the `Service` interface, or `ServicePerEndpoint`, is
 * {{{
 * trait LoggerServicePerEndpoint {
 *   val log: com.twitter.finagle.Service[Logger.Log.Args, Logger.Log.SuccessType]
 *   val getLogSize: com.twitter.finagle.Service[Logger.GetLogSize.Args, Logger.GetLogSize.SuccessType]
 * }
 * }}}
 *
 * and the method interface, or `MethodPerEndpoint`, is
 * {{{
 * trait Logger.MethodPerEndpoint {
 *   def log(message: String, logLevel: Int): Future[String]
 *   def getLogSize(): Future[Int]
 * }
 * }}}
 *
 * Service interfaces can be modified and composed with Finagle `Filters`.
 */
object ThriftServicePerEndpoint {

  /**
   * Build a Service from a given Thrift method.
   */
  def apply(
    method: ThriftMethod,
    thriftService: Service[ThriftClientRequest, Array[Byte]],
    clientParam: RichClientParam
  ): Service[method.Args, method.SuccessType] = {
    val stats: SimpleFilter[method.Args, method.SuccessType] =
      if (clientParam.perEndpointStats)
        statsFilter(method, clientParam.clientStats, clientParam.responseClassifier)
      else
        Filter.identity

    stats
      .andThen(ThriftCodec.filter(method, clientParam.protocolFactory))
      .andThen(thriftService)
  }

  def apply(
    method: ThriftMethod,
    thriftService: Service[ThriftClientRequest, Array[Byte]],
    pf: TProtocolFactory,
    stats: StatsReceiver
  ): Service[method.Args, method.SuccessType] =
    apply(method, thriftService, RichClientParam(pf, clientStats = stats))

  /**
   * A [[Filter]] that updates success and failure stats for a thrift method.
   * Responses are classified according to `responseClassifier`.
   */
  private def statsFilter(
    method: ThriftMethod,
    stats: StatsReceiver,
    responseClassifier: ResponseClassifier
  ): SimpleFilter[method.Args, method.SuccessType] = {
    val methodStats = ThriftMethodStats(stats.scope(method.serviceName).scope(method.name))
    val methodStatsResponseHandler = ThriftMethodStatsHandler(method) _
    new SimpleFilter[method.Args, method.SuccessType] {
      def apply(
        args: method.Args,
        service: Service[method.Args, method.SuccessType]
      ): Future[method.SuccessType] = {
        methodStats.requestsCounter.incr()
        service(args).respond { response =>
          methodStatsResponseHandler(responseClassifier, methodStats, args, response)
        }
      }
    }
  }

  def resultFilter(
    method: ThriftMethod
  ): Filter[method.Args, method.SuccessType, method.Args, method.Result] =
    new Filter[method.Args, method.SuccessType, method.Args, method.Result] {
      private[this] val responseFn: method.Result => Future[method.SuccessType] = { response =>
        response.firstException() match {
          case Some(exception) =>
            ThriftSourcedException.setServiceName(exception, method.serviceName)
            Future.exception(exception)
          case None =>
            response.successField match {
              case Some(result) =>
                Future.value(result)
              case None =>
                Future.exception(
                  new TApplicationException(
                    TApplicationException.MISSING_RESULT,
                    s"Thrift method '${method.name}' failed: missing result"
                  )
                )
            }
        }
      }

      def apply(
        args: method.Args,
        service: Service[method.Args, method.Result]
      ): Future[method.SuccessType] =
        service(args).flatMap(responseFn)
    }
}
