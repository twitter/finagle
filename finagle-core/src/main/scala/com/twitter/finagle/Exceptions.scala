package com.twitter.finagle

import com.twitter.finagle.context.RemoteInfo
import com.twitter.logging.{HasLogLevel, Level}
import com.twitter.util.Duration
import java.net.SocketAddress
import scala.util.control.NoStackTrace

/**
 * A trait for exceptions that contain remote information:
 * the downstream address/client id, upstream address/client id (if applicable), and trace id
 * of the request. [[RemoteInfo.NotAvailable]] is used if no remote information
 * has been set.
 */
trait HasRemoteInfo extends Exception {
  private[this] var _remoteInfo: RemoteInfo = RemoteInfo.NotAvailable

  def remoteInfo(): RemoteInfo = _remoteInfo

  private[finagle] def setRemoteInfo(remoteInfo: RemoteInfo): Unit =
    _remoteInfo = remoteInfo

  def exceptionMessage(): String = super.getMessage()

  override def getMessage(): String =
    if (exceptionMessage == null) null
    else s"$exceptionMessage. Remote Info: $remoteInfo"
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
  val UnspecifiedServiceName = "unspecified"

  def unapply(t: Throwable): Option[String] = t match {
    case sourced: SourcedException
      if sourced.serviceName != SourcedException.UnspecifiedServiceName =>
      Some(sourced.serviceName)
    case sourced: Failure =>
      sourced.getSource(Failure.Source.Service).map(_.toString)
    case _ =>
      None
  }
}

/**
 * A base class for request failures. Indicates that some failure occurred
 * before a request could be successfully serviced.
 */
class RequestException(message: String, cause: Throwable)
  extends Exception(message, cause)
  with NoStackTrace
  with SourcedException
{
  def this() = this(null, null)
  def this(cause: Throwable) = this(null, cause)
  override def getStackTrace = if (cause != null) cause.getStackTrace else super.getStackTrace
}

/**
 * Indicates that an operation exceeded some timeout duration before completing.
 * Differs from [[com.twitter.util.TimeoutException]] in that this trait doesn't
 * extend [[java.util.concurrent.TimeoutException]], provides more context in
 * its error message (e.g. the source and timeout value), and is only used
 * within the confines of Finagle.
 */
trait TimeoutException extends SourcedException { self: Exception =>
  protected val timeout: Duration
  protected def explanation: String

  override def exceptionMessage = s"exceeded $timeout to $serviceName while $explanation"
}

/**
 * Indicates that a request timed out. See
 * [[com.twitter.finagle.IndividualRequestTimeoutException]] and
 * [[com.twitter.finagle.GlobalRequestTimeoutException]] for details on the
 * different request granularities that this exception class can pertain to.
 */
class RequestTimeoutException(
  protected val timeout: Duration,
  protected val explanation: String
) extends RequestException with TimeoutException

/**
 * Indicates that a single Finagle-level request timed out. In contrast to
 * [[com.twitter.finagle.RequestTimeoutException]], an "individual request"
 * could be a single request-retry performed as a constituent of an
 * application-level RPC.
 */
class IndividualRequestTimeoutException(timeout: Duration)
  extends RequestTimeoutException(
    timeout,
    "waiting for a response for an individual request, excluding retries")

/**
 * Indicates that a request timed out, where "request" comprises a full RPC
 * from the perspective of the application. For instance, multiple retried
 * Finagle-level requests could constitute the single request that this
 * exception pertains to.
 */
class GlobalRequestTimeoutException(timeout: Duration)
  extends RequestTimeoutException(
    timeout,
    "waiting for a response for the request, including retries (if applicable)")

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
 * [1] http://twitter.github.io/finagle/guide/Names.html
 */
class NoBrokersAvailableException(
  val name: String,
  val baseDtab: Dtab,
  val localDtab: Dtab
) extends RequestException {
  def this(name: String = "unknown") = this(name, Dtab.empty, Dtab.empty)

  override def exceptionMessage =
    s"No hosts are available for $name, Dtab.base=[${baseDtab.show}], Dtab.local=[${localDtab.show}]"
}

/**
 * Indicates that a request was cancelled. Cancellation is propagated between a
 * Finagle server and a client intra-process when the server is interrupted by
 * an upstream service. In such cases, the pending Future is interrupted with
 * this exception. The client will cancel its pending request which will by
 * default propagate an interrupt to its downstream, and so on. This is done to
 * conserve resources.
 *
 * @see The [[http://twitter.github.io/finagle/guide/FAQ.html#what-are-cancelledrequestexception-and-cancelledconnectionexception user guide]]
 *      for additional details.
 */
class CancelledRequestException(cause: Throwable) extends RequestException(cause) {
  def this() = this(null)
  override def exceptionMessage = {
    if (cause == null)
      "request cancelled"
    else
      "request cancelled due to " + cause
  }
}

/**
 * Used by [[com.twitter.finagle.pool.WatermarkPool]] to indicate that a request
 * failed because too many requests are already waiting for a connection to
 * become available from a client's connection pool.
 */
class TooManyWaitersException extends RequestException

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
 * @see The [[http://twitter.github.io/finagle/guide/FAQ.html#what-are-cancelledrequestexception-and-cancelledconnectionexception user guide]]
 *      for additional details.
 */
class CancelledConnectionException(cause: Throwable) extends RequestException(cause) {
  def this() = this(null)
}

/**
 * Used by [[com.twitter.finagle.service.FailFastFactory]] to indicate that a
 * request failed because all hosts in the cluster to which the client is
 * connected have been marked as failed. See [[com.twitter.finagle.service.FailFastFactory]]
 * for details on this behavior.
 *
 * @see The [[http://twitter.github.io/finagle/guide/FAQ.html#why-do-clients-see-com-twitter-finagle-failedfastexception-s user guide]]
 *      for additional details.
 */
class FailedFastException(message: String)
  extends RequestException(message, cause = null)
  with WriteException
{
  def this() = this(null)
}

/**
 * Indicates that the request was not servable, according to some policy. See
 * [[com.twitter.finagle.service.OptionallyServableFilter]] as an example.
 */
class NotServableException extends RequestException

/**
 * Indicates that the client failed to distribute a given request according to
 * some sharding strategy. See [[com.twitter.finagle.service.ShardingService]]
 * for details on this behavior.
 */
class NotShardableException extends NotServableException

/**
 * Indicates that the shard to which a request was assigned was not available.
 * See [[com.twitter.finagle.service.ShardingService]] for details on this
 * behavior.
 */
class ShardNotAvailableException extends NotServableException

object ChannelException {
  def apply(cause: Throwable, remoteAddress: SocketAddress) = {
    cause match {
      case exc: ChannelException => exc
      case _: java.net.ConnectException                    => new ConnectionFailedException(cause, remoteAddress)
      case _: java.nio.channels.UnresolvedAddressException => new ConnectionFailedException(cause, remoteAddress)
      case _: java.nio.channels.ClosedChannelException     => new ChannelClosedException(cause, remoteAddress)
      case e: java.io.IOException
        if "Connection reset by peer" == e.getMessage      => new ChannelClosedException(cause, remoteAddress)
      case e: java.io.IOException
        if "Broken pipe" == e.getMessage                   => new ChannelClosedException(cause, remoteAddress)
      case e: java.io.IOException
        if "Connection timed out" == e.getMessage          => new ConnectionFailedException(cause, remoteAddress)
      case e                                               => new UnknownChannelException(cause, remoteAddress)
    }
  }
}

/**
 * An exception encountered within the context of a given socket channel.
 */
class ChannelException(underlying: Throwable, val remoteAddress: SocketAddress)
  extends Exception(underlying)
  with SourcedException
  with HasLogLevel
{
  def this(underlying: Throwable) = this(underlying, null)
  def this() = this(null, null)
  override def exceptionMessage = {
    val message = (underlying, remoteAddress) match {
      case (_, null) => super.exceptionMessage
      case (null, _) => s"ChannelException at remote address: ${remoteAddress.toString}"
      case (_, _) => s"${underlying.getMessage} at remote address: ${remoteAddress.toString}"
    }

    if (serviceName == SourcedException.UnspecifiedServiceName) message
    else s"$message from service: $serviceName"
  }
  def logLevel: Level = Level.DEBUG
}

/**
 * Indicates that the client failed to establish a connection. Typically this
 * class will be extended to provide additional information relevant to a
 * particular category of connection failure.
 */
class ConnectionFailedException(underlying: Throwable, remoteAddress: SocketAddress)
  extends ChannelException(underlying, remoteAddress) with NoStackTrace {
  def this() = this(null, null)
}

/**
 * Indicates that a given channel was closed, for instance if the connection
 * was reset by a peer or a proxy.
 */
class ChannelClosedException(underlying: Throwable, remoteAddress: SocketAddress)
  extends ChannelException(underlying, remoteAddress) with NoStackTrace {
  def this(remoteAddress: SocketAddress) = this(null, remoteAddress)
  def this() = this(null, null)
}

/**
 * Indicates that a write to a given `remoteAddress` timed out.
 */
class WriteTimedOutException(
    remoteAddress: SocketAddress) extends ChannelException(null, remoteAddress) {
  def this() = this(null)
}

/**
 * Indicates that a read from a given `remoteAddress` timed out.
 */
class ReadTimedOutException(
    remoteAddress: SocketAddress) extends ChannelException(null, remoteAddress) {
  def this() = this(null)
}

/**
 * Indicates that some client state was inconsistent with the observed state of
 * some server. For example, the client could receive a channel-connection event
 * from a proxy when there is no outstanding connect request.
 */
class InconsistentStateException(remoteAddress: SocketAddress) extends ChannelException(null, remoteAddress) {
  def this() = this(null)
}

/**
 * A catch-all exception class for uncategorized
 * [[com.twitter.finagle.ChannelException ChannelExceptions]].
 */
case class UnknownChannelException(underlying: Throwable, override val remoteAddress: SocketAddress)
  extends ChannelException(underlying, remoteAddress) {
  def this() = this(null, null)
}

object WriteException {
  def apply(underlying: Throwable): WriteException =
    ChannelWriteException(underlying)

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
case class ChannelWriteException(underlying: Throwable)
  extends ChannelException(underlying)
  with WriteException
  with NoStackTrace
{
  override def fillInStackTrace: NoStackTrace = this
  override def getStackTrace: Array[StackTraceElement] = underlying.getStackTrace
}

/**
 * Indicates that an error occurred while an SSL handshake was being performed
 * with a server at a given `remoteAddress`.
 */
case class SslHandshakeException(underlying: Throwable, override val remoteAddress: SocketAddress)
  extends ChannelException(underlying, remoteAddress) {
  def this() = this(null, null)
}

/**
 * Indicates that the certificate for a given session was invalidated.
 */
case class SslHostVerificationException(principal: String) extends ChannelException {
  def this() = this(null)
}

/**
 * Indicates that connecting to a given `remoteAddress` was refused.
 */
case class ConnectionRefusedException(override val remoteAddress: SocketAddress)
  extends ChannelException(null, remoteAddress) {
  def this() = this(null)
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
 * Indicates that a request failed because a
 * [[com.twitter.finagle.transport.Transport]] write associated with the request
 * was cancelled.
 */
class CancelledWriteException extends TransportException

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
{
  override protected def explanation =
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
 * [[org.jboss.netty.buffer.ChannelBuffer]].
 *
 * TODO: Probably remove this exception class once we migrate away from Netty
 * usage in public APIs.
 */
class ChannelBufferUsageException(description: String) extends Exception(description)

/**
 * An exception that is raised on requests that are discarded because
 * their corresponding backup requests succeeded first. See
 * [[com.twitter.finagle.exp.BackupRequestFilter]] for details.
 */
object BackupRequestLost extends Exception with NoStackTrace with HasLogLevel {
  def logLevel: Level = Level.TRACE
}
