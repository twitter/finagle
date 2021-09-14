package com.twitter.finagle.thrift.service

import com.twitter.finagle.context.Contexts
import com.twitter.finagle.service.ResponseClassifier
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.thrift.{Headers, RichClientParam, ThriftClientRequest, ThriftMethodStats}
import com.twitter.finagle.{Filter, Service, SimpleFilter}
import com.twitter.scrooge
import com.twitter.scrooge.ThriftMethod
import com.twitter.util.{Future, Return, Throw, Try}

/**
 * Construct `Service[scrooge.Request[method.Args],scrooge.Response[method.SuccessType]]` interface for a [[ThriftMethod]].
 *
 * There are two ways to use a Scrooge-generated Thrift `Service` with Finagle:
 *
 * 1. Using a Service interface, i.e. a collection of Finagle `Services`, e.g., ReqRepServicePerEndpoint.
 *
 * 2. Using a method interface, i.e. a collection of methods returning `Futures`, e.g, MethodPerEndpoint.
 *
 * Example: for a Thrift service IDL:
 * {{{
 * service Logger {
 *   string log(1: string message, 2: i32 logLevel);
 *   i32 getLogSize();
 * }
 * }}}
 *
 * the `Service` interface, or `ReqRepServicePerEndpoint`, is
 * {{{
 * trait LoggerServiceIface {
 *   val log: com.twitter.finagle.Service[scrooge.Request[Logger.Log.Args], scrooge.Response[Logger.Log.SuccessType]]
 *   val getLogSize: com.twitter.finagle.Service[[scrooge.Request[Logger.GetLogSize.Args], scrooge.Response[Logger.GetLogSize.SuccessType]]
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
 * ReqRepServicePerEndpoints can be modified and composed with Finagle `Filters`.
 */
object ThriftReqRepServicePerEndpoint {

  /* Public */

  /**
   * Build a `Service[scrooge.Request[method.Args],scrooge.Response[method.SuccessType]]` from a given [[ThriftMethod]].
   */
  def apply(
    method: ThriftMethod,
    thriftService: Service[ThriftClientRequest, Array[Byte]],
    clientParam: RichClientParam
  ): Service[scrooge.Request[method.Args], scrooge.Response[method.SuccessType]] = {
    val stats: SimpleFilter[scrooge.Request[method.Args], scrooge.Response[method.SuccessType]] =
      if (clientParam.perEndpointStats)
        statsFilter(method, clientParam.clientStats, clientParam.responseClassifier)
      else
        Filter.identity

    stats
      .andThen(reqRepFilter(method))
      .andThen(ThriftCodec.filter(method, clientParam.protocolFactory))
      .andThen(thriftService)
  }

  /**
   * Transform a `scrooge.Response[A]` to a `Future[A]`.
   *
   * @param result scrooge.Response to transform.
   *
   * @return a `Future[A]` of the result or exception.
   */
  def transformResult[A](result: Try[scrooge.Response[A]]): Future[A] = result match {
    case Return(response) =>
      // we rely on the Headers.Response.Values to be mutable such that we
      // can update the same reference in scope that will be read downstream.
      Contexts.local.get(Headers.Response.Key).foreach { responseCtx =>
        responseCtx.set(response.headers.toBufSeq)
      }
      Future.value(response.value)
    case Throw(e) =>
      Future.exception[A](e)
  }

  /* Private */

  /**
   * A [[Filter]] that updates success and failure stats for a [[ThriftMethod]].
   *
   * Responses are classified according to `responseClassifier`.
   */
  private def statsFilter(
    method: ThriftMethod,
    stats: StatsReceiver,
    responseClassifier: ResponseClassifier
  ): SimpleFilter[scrooge.Request[method.Args], scrooge.Response[method.SuccessType]] = {
    val methodStats = ThriftMethodStats(stats.scope(method.serviceName).scope(method.name))
    val methodStatsResponseHandler = ThriftMethodStatsHandler(method) _
    new SimpleFilter[scrooge.Request[method.Args], scrooge.Response[method.SuccessType]] {
      def apply(
        request: scrooge.Request[method.Args],
        service: Service[scrooge.Request[method.Args], scrooge.Response[method.SuccessType]]
      ): Future[scrooge.Response[method.SuccessType]] = {
        methodStats.requestsCounter.incr()
        service(request).respond { response: Try[scrooge.Response[method.SuccessType]] =>
          methodStatsResponseHandler(
            responseClassifier,
            methodStats,
            request.args,
            response.map(_.value)
          )
        }
      }
    }
  }

  private def reqRepFilter(method: ThriftMethod) =
    new Filter[scrooge.Request[method.Args], scrooge.Response[
      method.SuccessType
    ], method.Args, method.SuccessType] {
      def apply(
        request: scrooge.Request[method.Args],
        service: Service[method.Args, method.SuccessType]
      ): Future[scrooge.Response[method.SuccessType]] = {
        Contexts.local.let(
          Headers.Request.Key,
          Headers.Values(request.headers.toBufSeq),
          Headers.Response.Key,
          Headers.Response.newValues
        ) {
          service(request.args).transform {
            case Return(success) =>
              val responseCtx = Contexts.local(Headers.Response.Key)
              Future.value(scrooge.Response(responseCtx.values, success))
            case t @ Throw(_) =>
              Future.const(t.cast[scrooge.Response[method.SuccessType]])
          }
        }
      }
    }
}
