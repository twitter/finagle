package com.twitter.finagle

import com.twitter.finagle.IOExceptionStrings.{
  ChannelClosedStrings,
  ConnectionFailedStrings,
  ChannelClosedSslExceptionMessages
}
import com.twitter.finagle.context.RemoteInfo
import com.twitter.logging.{HasLogLevel, Level}
import com.twitter.util.Duration
import java.net.SocketAddress
import javax.net.ssl.{SSLException => JSSLException}
import scala.util.control.NoStackTrace

/**
 * A trait for exceptions that contain remote information:
 * the downstream address/client id, upstream address/client id (if applicable), and trace id
 * of the request. [[RemoteInfo.NotAvailable]] is used if no remote information
 * has been set.
 */
trait HasRemoteInfo extends Exception {
  @volatile private[this] var _remoteInfo: RemoteInfo = RemoteInfo.NotAvailable

  def remoteInfo(): RemoteInfo = _remoteInfo

  private[finagle] def setRemoteInfo(remoteInfo: RemoteInfo): Unit =
    _remoteInfo = remoteInfo

  def exceptionMessage(): String = super.getMessage()

  override def getMessage(): String =
    if (exceptionMessage() == null) null
    else s"${exceptionMessage()}. Remote Info: ${remoteInfo()}"
}

/**
 * A trait for exceptions that have a source. The name of the source is
 * specified as a `serviceName`. The "unspecified" value is used if no
 * `serviceName` is provided by the implementation.
 */
trait SourcedException extends Exception with HasRemoteInfo {
  var serviceName: String = SourcedException.UnspecifiedServiceName
}

object SourcedException {
  val UnspecifiedServiceName: String = "unspecified"

  def unapply(t: Throwable): Option[String] = t match {
    case sourced: SourcedException
        if sourced.serviceName != SourcedException.UnspecifiedServiceName =>
      Some(sourced.serviceName)
    case sourced: Failure =>
      sourced.getSource(Failure.Source.Service).map(_.toString)
    case _ =>
      None
  }

  /**
   * Given a Throwable and a serviceName, if the Throwable is a SourcedException
   * and the serviceName is not empty, sets the serviceName on the Throwable
   * */
  def setServiceName(ex: Throwable, serviceName: String): Throwable = {
    if (serviceName != "" && ex.isInstanceOf[SourcedException]) {
      ex.asInstanceOf[SourcedException].serviceName = serviceName
    }
    ex
  }
}

/**
 * A base class for request failures. Indicates that some failure occurred
 * before a request could be successfully serviced.
 */
class RequestException(message: String, cause: Throwable)
    extends Exception(message, cause)
    with NoStackTrace
    with SourcedException {
  def this() = this(null, null)
  def this(cause: Throwable) = this(null, cause)
  override def getStackTrace: Array[StackTraceElement] =
    if (cause != null) cause.getStackTrace else super.getStackTrace
}

/**
 * Indicates that an operation exceeded some timeout duration before completing.
 * Differs from [[com.twitter.util.TimeoutException]] in that this trait doesn't
 * extend [[java.util.concurrent.TimeoutException]], provides more context in
 * its error message (e.g. the source and timeout value), and is only used
 * within the confines of Finagle.
 */
trait TimeoutException extends SourcedException with HasLogLevel { self: Exception =>
  protected val timeout: Duration
  protected def explanation: String
  def logLevel: Level = Level.TRACE

  override def exceptionMessage: String = s"exceeded $timeout to $serviceName while $explanation"
}

/**
 * Indicates that a request timed out. See
 * [[com.twitter.finagle.IndividualRequestTimeoutException]] and
 * [[com.twitter.finagle.GlobalRequestTimeoutException]] for details on the
 * different request granularities that this exception class can pertain to.
 */
class RequestTimeoutException(protected val timeout: Duration, protected val explanation: String)
    extends RequestException
    with TimeoutException

/**
 * Indicates that a single Finagle-level request timed out. In contrast to
 * [[com.twitter.finagle.RequestTimeoutException]], an "individual request"
 * could be a single request-retry performed as a constituent of an
 * application-level RPC.
 */
class IndividualRequestTimeoutException(timeout: Duration)
    extends RequestTimeoutException(
      timeout,
      "waiting for a response for an individual request, excluding retries"
    )

/**
 * Indicates that a request timed out, where "request" comprises a full RPC
 * from the perspective of the application. For instance, multiple retried
 * Finagle-level requests could constitute the single request that this
 * exception pertains to.
 */
class GlobalRequestTimeoutException(timeout: Duration)
    extends RequestTimeoutException(
      timeout,
      "waiting for a response for the request, including retries (if applicable)"
    )

/**
 * Indicates that a request failed because no servers were available. The
 * Finagle client's internal load balancer was empty. This typically occurs
 * under one of the following conditions:
 *
 * - The cluster is actually down. No servers are available.
 * - A service discovery failure. This can be due to a number of causes, such as
 *   the client being constructed with an invalid cluster destination name [1]
 *   or a failure in the service discovery system (e.g. DNS, ZooKeeper).
 *
 * A good way to diagnose NoBrokersAvailableExceptions is to reach out to the
 * owners of the service to which the client is attempting to connect and verify
 * that the service is operational. If so, then investigate the service
 * discovery mechanism that the client is using (e.g. the
 * [[com.twitter.finagle.Resolver]] that is it configured to use and the system
 * backing it).
 *
 * [1] https://twitter.github.io/finagle/guide/Names.html
 */
class NoBrokersAvailableException(
  val name: String,
  val baseDtabFn: () => Dtab,
  val localDtabFn: () => Dtab,
  val limitedDtabFn: () => Dtab)
    extends RequestException
    with SourcedException {

  // backwards compatibility constructor
  def this(name: String, baseDtab: Dtab, localDtab: Dtab, limitedDtab: Dtab) =
    this(name, () => baseDtab, () => localDtab, () => limitedDtab)

  def this(name: String = "unknown") =
    this(name, () => Dtab.base, () => Dtab.local, () => Dtab.limited)

  def baseDtab: Dtab = baseDtabFn()
  def localDtab: Dtab = localDtabFn()
  def limitedDtab: Dtab = limitedDtabFn()

  override def exceptionMessage: String =
    s"No hosts are available for $name, Dtab.base=[${baseDtab.show}], Dtab.limited=[${limitedDtab.show}], Dtab.local=[${localDtab.show}]"

  serviceName = name
}

/**
 * Indicates that a request was cancelled. Cancellation is propagated between a
 * Finagle server and a client intra-process when the server is interrupted by
 * an upstream service. In such cases, the pending Future is interrupted with
 * this exception. The client will cancel its pending request which will by
 * default propagate an interrupt to its downstream, and so on. This is done to
 * conserve resources.
 *
 * @see The [[https://twitter.github.io/finagle/guide/FAQ.html#what-are-cancelledrequestexception-and-cancelledconnectionexception user guide]]
 *      for additional details.
 */
class CancelledRequestException(cause: Throwable) extends RequestException(cause) with HasLogLevel {
  def this() = this(null)
  override def exceptionMessage(): String = {
    if (cause == null)
      "request cancelled"
    else
      "request cancelled due to " + cause
  }
  def logLevel: Level = Level.DEBUG
}

/**
 * Used by [[com.twitter.finagle.pool.WatermarkPool]] to indicate that a request
 * failed because too many requests are already waiting for a connection to
 * become available from a client's connection pool.
 */
class TooManyWaitersException extends RequestException with HasLogLevel {
  def logLevel: Level = Level.DEBUG
}

/**
 * A Future is satisfied with this exception when the process of establishing
 * a session is interrupted. Sessions are not preemptively established in Finagle,
 * rather requests are taxed with session establishment when necessary.
 * For example, this exception can occur if a request is interrupted while waiting for
 * an available session or if an interrupt is propagated from a Finagle server
 * during session establishment.
 *
 * @see com.twitter.finagle.CancelledRequestException
 *
 * @see The [[https://twitter.github.io/finagle/guide/FAQ.html#what-are-cancelledrequestexception-and-cancelledconnectionexception user guide]]
 *      for additional details.
 */
class CancelledConnectionException(cause: Throwable)
    extends RequestException(cause)
    with HasLogLevel {
  def this() = this(null)
  def logLevel: Level = Level.DEBUG
}

/**
 * Used by [[com.twitter.finagle.service.FailFastFactory]] to indicate that a
 * request failed because all hosts in the cluster to which the client is
 * connected have been marked as failed. See [[com.twitter.finagle.service.FailFastFactory]]
 * for details on this behavior.
 *
 * @see The [[https://twitter.github.io/finagle/guide/FAQ.html#why-do-clients-see-com-twitter-finagle-failedfastexception-s user guide]]
 *      for additional details.
 */
class FailedFastException(message: String, cause: Throwable, val flags: Long = FailureFlags.Empty)
    extends RequestException(message, cause)
    with WriteException
    with HasLogLevel
    with FailureFlags[FailedFastException] {

  def this(message: String, cause: Throwable) = this(message, cause, FailureFlags.Empty)

  def this(message: String) = this(message, null)

  def this() = this(null, null)

  protected def copyWithFlags(newFlags: Long): FailedFastException =
    new FailedFastException(message, cause, newFlags)

  def logLevel: Level = Level.DEBUG
}

/**
 * Indicates that the request was not servable, according to some policy. See
 * [[com.twitter.finagle.service.OptionallyServableFilter]] as an example.
 */
class NotServableException extends RequestException

/**
 * Indicates that the shard to which a request was assigned was not available.
 * See [[com.twitter.finagle.partitioning.PartitioningService]] for details on this
 * behavior.
 */
class ShardNotAvailableException extends NotServableException

object ChannelException {
  def apply(cause: Throwable, remoteAddress: SocketAddress): ChannelException = {
    cause match {
      case exc: ChannelException => exc
      case _: java.net.ConnectException => new ConnectionFailedException(cause, remoteAddress)
      case _: java.nio.channels.UnresolvedAddressException =>
        new ConnectionFailedException(cause, remoteAddress)
      case _: java.nio.channels.ClosedChannelException =>
        new ChannelClosedException(cause, remoteAddress)
      case e: JSSLException if ChannelClosedSslExceptionMessages.contains(e.getMessage) =>
        new ChannelClosedException(cause, remoteAddress)
      case e: JSSLException => new SslException(e, remoteAddress)
      case e: java.io.IOException if ChannelClosedStrings.contains(e.getMessage) =>
        new ChannelClosedException(cause, remoteAddress)
      case e: java.io.IOException if ConnectionFailedStrings.contains(e.getMessage) =>
        new ConnectionFailedException(cause, remoteAddress)
      case _ => new UnknownChannelException(cause, remoteAddress)
    }
  }
}

/**
 * An exception encountered within the context of a given socket channel.
 */
class ChannelException(underlying: Option[Throwable], remoteAddr: Option[SocketAddress])
    extends Exception(underlying.orNull)
    with SourcedException
    with HasLogLevel {
  def this(underlying: Throwable, remoteAddress: SocketAddress) =
    this(Option(underlying), Option(remoteAddress))

  def this(underlying: Throwable) = this(Option(underlying), None)
  def this() = this(None, None)
  override def exceptionMessage(): String = {
    val message = remoteAddr match {
      case None =>
        super.exceptionMessage()
      case Some(ra) =>
        underlying match {
          case None => s"ChannelException at remote address: $ra"
          case Some(t) => s"${t.getMessage} at remote address: $ra"
        }
    }

    if (serviceName == SourcedException.UnspecifiedServiceName) message
    else s"$message from service: $serviceName"
  }
  def logLevel: Level = Level.DEBUG

  /**
   * The `SocketAddress` of the remote peer.
   *
   * @return `null` if not available.
   */
  def remoteAddress: SocketAddress = remoteAddr.orNull

}

/**
 * Indicates that either SOCKS or HTTP(S) proxy server rejected client's connect request.
 */
class ProxyConnectException(
  message: String,
  remoteAddress: SocketAddress,
  val flags: Long = FailureFlags.NonRetryable)
    extends Exception(message)
    with NoStackTrace
    with FailureFlags[ProxyConnectException] {

  protected def copyWithFlags(flags: Long): ProxyConnectException =
    new ProxyConnectException(message, remoteAddress, flags)

  override def getMessage: String = s"Proxy connect to $remoteAddress failed with: $message"
}

/**
 * Indicates that the client failed to establish a connection. Typically this
 * class will be extended to provide additional information relevant to a
 * particular category of connection failure.
 */
class ConnectionFailedException(underlying: Option[Throwable], remoteAddress: Option[SocketAddress])
    extends ChannelException(underlying, remoteAddress) {
  def this(underlying: Throwable, remoteAddress: SocketAddress) =
    this(Option(underlying), Option(remoteAddress))
  def this() = this(None, None)
}

/**
 * Indicates that a given channel was closed, for instance if the connection
 * was reset by a peer or a proxy.
 */
class ChannelClosedException private[finagle] (
  underlying: Option[Throwable],
  remoteAddress: Option[SocketAddress],
  val flags: Long)
    extends ChannelException(underlying, remoteAddress)
    with FailureFlags[ChannelClosedException] {

  def this(underlying: Option[Throwable], remoteAddress: Option[SocketAddress]) =
    this(underlying, remoteAddress, FailureFlags.Empty)

  def this(underlying: Throwable, remoteAddress: SocketAddress) =
    this(Option(underlying), Option(remoteAddress))
  def this(remoteAddress: SocketAddress) = this(None, Option(remoteAddress))
  def this() = this(None, None)

  protected def copyWithFlags(flags: Long): ChannelClosedException =
    new ChannelClosedException(underlying, remoteAddress, flags)
}

/**
 * Indicates that a given stream was closed, for instance if the stream
 * was reset by a peer or a proxy.
 */
abstract class StreamClosedException(
  remoteAddress: Option[SocketAddress],
  streamId: Long,
  val flags: Long)
    extends ChannelException(None, remoteAddress)
    with FailureFlags[StreamClosedException]
    with NoStackTrace {

  def this(remoteAddress: Option[SocketAddress], streamId: Int) =
    this(remoteAddress, streamId, FailureFlags.Empty)

  /** Description of why this stream failed */
  protected def whyFailed: String

  override def exceptionMessage(): String = {
    if (whyFailed == null) s"Stream: $streamId was closed at remote address: $remoteAddress"
    else s"Stream: $streamId was closed at remote address: $remoteAddress, because $whyFailed"
  }
}

/**
 * Indicates that a write to a given `remoteAddress` timed out.
 */
class WriteTimedOutException(remoteAddress: Option[SocketAddress])
    extends ChannelException(None, remoteAddress) {
  def this(remoteAddress: SocketAddress) = this(Option(remoteAddress))
  def this() = this(None)
}

/**
 * Indicates that a read from a given `remoteAddress` timed out.
 */
class ReadTimedOutException(remoteAddress: Option[SocketAddress])
    extends ChannelException(None, remoteAddress) {
  def this(remoteAddress: SocketAddress) = this(Option(remoteAddress))
  def this() = this(None)
}

/**
 * Indicates that some client state was inconsistent with the observed state of
 * some server. For example, the client could receive a channel-connection event
 * from a proxy when there is no outstanding connect request.
 */
class InconsistentStateException(remoteAddress: Option[SocketAddress])
    extends ChannelException(None, remoteAddress) {
  def this(remoteAddress: SocketAddress) = this(Option(remoteAddress))
  def this() = this(None)
}

/**
 * A catch-all exception class for uncategorized
 * [[com.twitter.finagle.ChannelException ChannelExceptions]].
 */
case class UnknownChannelException(ex: Option[Throwable], remoteAddr: Option[SocketAddress])
    extends ChannelException(ex, remoteAddr) {
  def this(underlying: Throwable, remoteAddress: SocketAddress) =
    this(Option(underlying), Option(remoteAddress))
  def this() = this(None, None)

  /**
   * The cause of this exception, or `null` if there is no cause.
   */
  def underlying: Throwable = ex.orNull
}

object WriteException {
  def apply(underlying: Throwable): WriteException =
    new ChannelWriteException(underlying)

  def unapply(t: Throwable): Option[Throwable] = t match {
    case we: WriteException => Some(we.getCause)
    case _ => None
  }
}

/**
 * Marker trait to indicate there was an exception before writing any of the
 * request.
 * These exceptions should generally be retryable.
 *
 * @see [[com.twitter.finagle.service.RetryPolicy.RetryableWriteException]]
 * @see [[com.twitter.finagle.service.RetryPolicy.WriteExceptionsOnly]]
 */
trait WriteException extends Exception with SourcedException

/**
 * Default implementation for [[WriteException]] that wraps an underlying exception.
 */
case class ChannelWriteException(ex: Option[Throwable])
    extends ChannelException(ex, None)
    with WriteException
    with NoStackTrace {
  def this(underlying: Throwable) = this(Option(underlying))
  override def fillInStackTrace: NoStackTrace = this
  override def getStackTrace: Array[StackTraceElement] =
    ex match {
      case Some(u) => u.getStackTrace
      case None => Array.empty
    }

  /**
   * The cause of this exception, or `null` if there is no cause.
   */
  def underlying: Throwable = ex.orNull
}

object ChannelWriteException {
  def apply(underlying: Throwable): ChannelWriteException =
    new ChannelWriteException(Option(underlying))
}

/**
 * Indicates that an SSL/TLS exception occurred.
 */
class SslException(cause: Option[Throwable], remoteAddr: Option[SocketAddress])
    extends ChannelException(cause, remoteAddr) {
  def this(cause: JSSLException, remoteAddr: SocketAddress) =
    this(Option(cause), Option(remoteAddr))
}

/**
 * Indicates that an error occurred while `SslClientSessionVerification` was
 * being performed, or the server disconnected from the client in a way that
 * indicates that there was high probability that the server failed to verify
 * the client's certificate.
 */
case class SslVerificationFailedException(ex: Option[Throwable], remoteAddr: Option[SocketAddress])
    extends SslException(ex, remoteAddr) {
  def this(underlying: Throwable, remoteAddress: SocketAddress) =
    this(Option(underlying), Option(remoteAddress))
  def this() = this(None, None)

  /**
   * The cause of this exception, or `null` if there is no cause.
   */
  def underlying: Throwable = ex.orNull
}

/**
 * Indicates that connecting to a given `remoteAddress` was refused.
 */
case class ConnectionRefusedException(remoteAddr: Option[SocketAddress])
    extends ChannelException(None, remoteAddr) {
  def this(remoteAddress: SocketAddress) = this(Option(remoteAddress))
  def this() = this(None)
}

/**
 * Indicates that requests were failed by a rate-limiter. See
 * [[com.twitter.finagle.service.RateLimitingFilter]] for details.
 */
case class RefusedByRateLimiter() extends ChannelException

/**
 * A base class for exceptions encountered in the context of a
 * [[com.twitter.finagle.transport.Transport]].
 */
class TransportException extends Exception with SourcedException

/**
 * Indicates that a [[com.twitter.finagle.transport.Transport]] write associated
 * with the request was dropped by the transport (usually to respect backpressure).
 */
class DroppedWriteException extends TransportException

/**
 * A trait for exceptions related to a [[com.twitter.finagle.Service]].
 */
trait ServiceException extends Exception with SourcedException

/**
 * Indicates that a request was applied to a [[com.twitter.finagle.Service]]
 * that is closed (i.e. the connection is closed).
 */
class ServiceClosedException extends ServiceException

/**
 * Indicates that this service was closed and returned to the underlying pool.
 */
class ServiceReturnedToPoolException(val flags: Long = FailureFlags.NonRetryable)
    extends IllegalStateException
    with ServiceException
    with HasLogLevel
    with FailureFlags[ServiceReturnedToPoolException] {

  protected def copyWithFlags(newFlags: Long): ServiceReturnedToPoolException =
    new ServiceReturnedToPoolException(newFlags)

  def logLevel: Level = Level.WARNING
}

/**
 * Indicates that a request was applied to a [[com.twitter.finagle.Service]]
 * that is unavailable. This constitutes a fail-stop condition.
 */
class ServiceNotAvailableException extends ServiceException

/**
 * Indicates that the connection was not established within the timeouts.
 * This type of exception should generally be safe to retry.
 */
class ServiceTimeoutException(override protected val timeout: Duration)
    extends WriteException
    with ServiceException
    with TimeoutException
    with NoStackTrace {
  override protected def explanation: String =
    "creating a service/connection or reserving a service/connection from the service/connection pool " + serviceName
}

/**
 * A base class for exceptions encountered on account of incorrect API usage.
 */
class ApiException extends Exception

/**
 * Indicates that the client has issued more concurrent requests than are
 * allowable, where "allowable" is typically determined based on some
 * configurable maximum.
 */
class TooManyConcurrentRequestsException extends ApiException

/**
 * Indicates that an error occurred on account of incorrect usage of a
 * `io.netty.buffer.ByteBuf`.
 *
 * TODO: Probably remove this exception class once we migrate away from Netty
 * usage in public APIs.
 */
class ChannelBufferUsageException(description: String) extends Exception(description)
