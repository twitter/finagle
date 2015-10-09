package com.twitter.finagle.thrift

import com.twitter.app.GlobalFlag
import com.twitter.conversions.storage._
import com.twitter.finagle.stats.{ClientStatsReceiver, Counter, StatsReceiver}
import com.twitter.finagle._
import com.twitter.scrooge._
import com.twitter.util._
import java.util.Arrays
import org.apache.thrift.TApplicationException
import org.apache.thrift.protocol.{TMessageType, TMessage, TProtocolFactory}
import org.apache.thrift.transport.TMemoryInputTransport


object maxReusableBufferSize extends GlobalFlag[StorageUnit](
  16.kilobytes,
  "Max size (bytes) for ThriftServiceIface reusable transport buffer"
)

/**
 * Typeclass ServiceIfaceBuilder[T] creates T-typed interfaces from thrift clients.
 * Scrooge generates implementations of this builder.
 */
trait ServiceIfaceBuilder[ServiceIface] {
  /**
   * Build a client ServiceIface wrapping a binary thrift service.
   *
   * @param thriftService An underlying thrift service that works on byte arrays.
   * @param pf The protocol factory used to encode/decode thrift structures.
   */
  def newServiceIface(
    thriftService: Service[ThriftClientRequest, Array[Byte]],
    pf: TProtocolFactory,
    stats: StatsReceiver
  ): ServiceIface
}

/**
 * A typeclass to construct a MethodIface by wrapping a ServiceIface.
 * This is a compatibility constructor to replace an existing Future interface
 * with one built from a ServiceIface.
 *
 * Scrooge generates implementations of this builder.
 */
trait MethodIfaceBuilder[ServiceIface, MethodIface] {
  /**
   * Build a FutureIface wrapping a ServiceIface.
   */
  def newMethodIface(serviceIface: ServiceIface): MethodIface
}

object ThriftMethodStats {
  def apply(stats: StatsReceiver): ThriftMethodStats =
    ThriftMethodStats(
      stats.counter("requests"),
      stats.counter("success"),
      stats.counter("failures"),
      stats.scope("failures"))
}

case class ThriftMethodStats(
  requestsCounter: Counter,
  successCounter: Counter,
  failuresCounter: Counter,
  failuresScope: StatsReceiver)

/**
 * Construct Service interface for a thrift method.
 *
 * There are two ways to use a Scrooge-generated thrift service with Finagle:
 *
 * 1. Using a Service interface, i.e. a collection of finagle [[Service Services]].
 *
 * 2. Using a method interface, i.e. a collection of methods returning [[Future Futures]].
 *
 * Example: for a thrift service
 * {{{
 * service Logger {
 *   string log(1: string message, 2: i32 logLevel);
 *   i32 getLogSize();
 * }
 * }}}
 *
 * the Service interface is
 * {{{
 * trait LoggerServiceIface {
 *   val log: com.twitter.finagle.Service[Logger.Log.Args, Logger.Log.Result]
 *   val getLogSize: com.twitter.finagle.Service[Logger.GetLogSize.Args, Logger.GetLogSize.Result]
 *  }
 * }}}
 *
 * and the method interface is
 * {{{
 * trait Logger[Future] {
 *   def log(message: String, logLevel: Int): Future[String]
 *   def getLogSize(): Future[Int]
 * }
 * }}}
 *
 * Service interfaces can be modified and composed with Finagle [[Filter Filters]].
 */
object ThriftServiceIface {
  private val resetCounter = ClientStatsReceiver.scope("thrift_service_iface").counter("reusable_buffer_resets")

  /**
   * Build a Service from a given Thrift method.
   */
  def apply(
    method: ThriftMethod,
    thriftService: Service[ThriftClientRequest, Array[Byte]],
    pf: TProtocolFactory,
    stats: StatsReceiver
  ): Service[method.Args, method.Result] = {
    statsFilter(method, stats) andThen
      thriftCodecFilter(method, pf) andThen
      thriftService
  }

  /**
   * A [[Filter]] that updates success and failure stats for a thrift method.
   * Thrift exceptions are counted as failures here.
   */
  private def statsFilter(
    method: ThriftMethod,
    stats: StatsReceiver
  ): SimpleFilter[method.Args, method.Result] = {
    val methodStats = ThriftMethodStats(stats.scope(method.serviceName).scope(method.name))
    new SimpleFilter[method.Args, method.Result] {
      def apply(
        args: method.Args,
        service: Service[method.Args, method.Result]
      ): Future[method.Result] = {
        methodStats.requestsCounter.incr()
        service(args).onSuccess { result =>
          if (result.successField.isDefined) {
            methodStats.successCounter.incr()
          } else {
            result.firstException.map { ex =>
              methodStats.failuresCounter.incr()
              methodStats.failuresScope.counter(Throwables.mkString(ex): _*).incr()
            }
          }
        }
      }
    }
  }

  /**
   * A [[Filter]] that wraps a binary thrift Service[ThriftClientRequest, Array[Byte]]
   * and produces a [[Service]] from a [[ThriftStruct]] to [[ThriftClientRequest]] (i.e. bytes).
   */
  private def thriftCodecFilter(
    method: ThriftMethod,
    pf: TProtocolFactory
  ): Filter[method.Args, method.Result, ThriftClientRequest, Array[Byte]] =
    new Filter[method.Args, method.Result, ThriftClientRequest, Array[Byte]] {
      override def apply(
        args: method.Args,
        service: Service[ThriftClientRequest, Array[Byte]]
      ): Future[method.Result] = {
        val request = encodeRequest(method.name, args, pf, method.oneway)
        service(request).map { bytes =>
          decodeResponse(bytes, method.responseCodec, pf)
        }
      }
    }

  def resultFilter(
    method: ThriftMethod
  ): Filter[method.Args, method.SuccessType, method.Args, method.Result] =
    new Filter[method.Args, method.SuccessType, method.Args, method.Result] {
      def apply(
        args: method.Args,
        service: Service[method.Args, method.Result]
      ): Future[method.SuccessType] = {
        service(args).flatMap { response: method.Result =>
          response.firstException() match {
            case Some(exception) =>
              setServiceName(exception, method.serviceName)
              Future.exception(exception)
            case None =>
              response.successField match {
                case Some(result) =>
                  Future.value(result)
                case None =>
                  Future.exception(new TApplicationException(
                    TApplicationException.MISSING_RESULT,
                    "Thrift method '${method.name}' failed: missing result"
                  ))
              }
          }
        }
      }
    }

  private[this] val tlReusableBuffer = new ThreadLocal[TReusableMemoryTransport] {
    override def initialValue() = TReusableMemoryTransport(512)
  }

  private[this] def getReusableBuffer(): TReusableMemoryTransport = {
    val buf = tlReusableBuffer.get()
    buf.reset()
    buf
  }

  private[this] def resetBuffer(trans: TReusableMemoryTransport): Unit = {
    if (trans.currentCapacity > maxReusableBufferSize().inBytes) {
      resetCounter.incr()
      tlReusableBuffer.remove()
    }
  }

  private def encodeRequest(
    methodName: String,
    args: ThriftStruct,
    pf: TProtocolFactory,
    oneway: Boolean
  ): ThriftClientRequest = {
    val buf = getReusableBuffer()
    val oprot = pf.getProtocol(buf)

    oprot.writeMessageBegin(new TMessage(methodName, TMessageType.CALL, 0))
    args.write(oprot)
    oprot.writeMessageEnd()

    val bytes = Arrays.copyOfRange(buf.getArray, 0, buf.length)
    resetBuffer(buf)

    new ThriftClientRequest(bytes, oneway)
  }

  private def decodeResponse[T <: ThriftStruct](
    resBytes: Array[Byte],
    codec: ThriftStructCodec[T],
    pf: TProtocolFactory
  ): T = {
    val iprot = pf.getProtocol(new TMemoryInputTransport(resBytes))
    val msg = iprot.readMessageBegin()
    if (msg.`type` == TMessageType.EXCEPTION) {
      val exception = TApplicationException.read(iprot)
      iprot.readMessageEnd()
      throw exception
    } else {
      val result = codec.decode(iprot)
      iprot.readMessageEnd()
      result
    }
  }

  private def setServiceName(ex: Throwable, serviceName: String): Throwable =
    ex match {
      case se: SourcedException if !serviceName.isEmpty =>
        se.serviceName = serviceName
        se
      case _ => ex
    }
}
