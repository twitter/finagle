package com.twitter.finagle.thrift

import com.twitter.app.GlobalFlag
import com.twitter.conversions.storage._
import com.twitter.finagle._
import com.twitter.finagle.service.ResponseClassifier
import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.thrift.service.{ThriftCodec, ThriftMethodStatsHandler, ThriftSourcedException}
import com.twitter.scrooge._
import com.twitter.util._
import org.apache.thrift.TApplicationException
import org.apache.thrift.protocol.TProtocolFactory

object maxReusableBufferSize
    extends GlobalFlag[StorageUnit](
      16.kilobytes,
      "Max size (bytes) for ThriftServiceIface reusable transport buffer"
    )

/**
 * Typeclass ServiceIfaceBuilder[T] creates T-typed interfaces from thrift clients.
 * Scrooge generates implementations of this builder.
 */
@deprecated("Use com.twitter.finagle.thrift.service.ServicePerEndpointBuilder", "2017-11-13")
trait ServiceIfaceBuilder[ServiceIface <: ThriftServiceIface.Filterable[ServiceIface]] {

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
 * the `Service` interface, or `ServiceIface`, is
 * {{{
 * trait LoggerServiceIface {
 *   val log: com.twitter.finagle.Service[Logger.Log.Args, Logger.Log.SuccessType]
 *   val getLogSize: com.twitter.finagle.Service[Logger.GetLogSize.Args, Logger.GetLogSize.SuccessType]
 * }
 * }}}
 *
 * and the method interface, or `MethodIface`, is
 * {{{
 * trait Logger[Future] {
 *   def log(message: String, logLevel: Int): Future[String]
 *   def getLogSize(): Future[Int]
 * }
 * }}}
 *
 * Service interfaces can be modified and composed with Finagle `Filters`.
 */
object ThriftServiceIface { // TODO: Rename ThriftServicePerEndpoint and move to com.twitter.finagle.thrift.service

  @deprecated("Use com.twitter.finagle.thrift.service.Filterable", "2017-11-06")
  type Filterable[+T] = com.twitter.finagle.thrift.service.Filterable[T]

  /**
   * Build a Service from a given Thrift method.
   */
  def apply(
    method: ThriftMethod,
    thriftService: Service[ThriftClientRequest, Array[Byte]],
    clientParam: RichClientParam
  ): Service[method.Args, method.SuccessType] =
    statsFilter(method, clientParam.clientStats, clientParam.responseClassifier)
      .andThen(ThriftCodec.filter(method, clientParam.protocolFactory))
      .andThen(thriftService)

  @deprecated("Use com.twitter.finagle.thrift.RichClientParam", "2017-08-16")
  def apply(
    method: ThriftMethod,
    thriftService: Service[ThriftClientRequest, Array[Byte]],
    pf: TProtocolFactory,
    stats: StatsReceiver,
    responseClassifier: ResponseClassifier
  ): Service[method.Args, method.SuccessType] = {
    apply(method, thriftService, RichClientParam(pf, clientStats = stats, responseClassifier = responseClassifier))
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
