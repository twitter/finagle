package com.twitter.finagle

import com.twitter.logging.{HasLogLevel, Level}
import com.twitter.util.Duration
import java.net.SocketAddress

/**
 * A trait for exceptions that have a source. The name of the source is
 * specified as a `serviceName`. The "unspecified" value is used if no
 * `serviceName` is provided by the implementation.
 */
trait SourcedException extends Exception {
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
 * A trait for common exceptions that either
 *
 * a) don't benefit from full stacktrace information (e.g. stacktraces don't add
 *    useful information, as in the case of connection closure), or
 * b) are thrown frequently enough that stacktrace-creation becomes unacceptably
 *    expensive.
 *
 * This trait represents a tradeoff between debugging ease and efficiency.
 * Implementers beware.
 */
trait NoStacktrace extends Exception {
  override def fillInStackTrace = this
  // specs expects non-empty stacktrace array
  this.setStackTrace(NoStacktrace.NoStacktraceArray)
}

object NoStacktrace {
  val NoStacktraceArray = Array(new StackTraceElement("com.twitter.finagle", "NoStacktrace", null, -1))
}

/**
 * A base class for request failures. Indicates that some failure occurred
 * before a request could be successfully serviced.
 */
class RequestException(message: String, cause: Throwable)
  extends Exception(message, cause)
  with NoStacktrace
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

  override def getMessage = s"exceeded $timeout to $serviceName while $explanation"
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

  override def getMessage =
    s"No hosts are available for $name, Dtab.base=[${baseDtab.show}], Dtab.local=[${localDtab.show}]"
}

@deprecated("no longer used by com.twitter.finagle.service.RetryExceptionsFilter", "7.0.0")
class RetryFailureException(cause: Throwable) extends RequestException(cause)

/**
 * Indicates that a request was cancelled. Cancellation is propagated between a
 * Finagle server and a client intra-process when the server is interrupted by
 * an upstream service. In such cases, the pending Future is interrupted with
 * this exception. The client will cancel its pending request which will by
 * default propagate an interrupt to its downstream, and so on. This is done to
 * conserve resources.
 */
class CancelledRequestException(cause: Throwable) extends RequestException(cause) {
  def this() = this(null)
  override def getMessage = {
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
 */
class CancelledConnectionException(cause: Throwable) extends RequestException(cause) {
  def this() = this(null)
}

@deprecated("no longer used by com.twitter.finagle.service.RetryFilter", "7.0.0")
class ReplyCastException extends RequestException

/**
 * Used by [[com.twitter.finagle.service.FailFastFactory]] to indicate that a
 * request failed because all hosts in the cluster to which the client is
 * connected have been marked as failed. See FailFastFactory for details on
 * this behavior.
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
  extends Exception(underlying) with SourcedException
{
  def this(underlying: Throwable) = this(underlying, null)
  def this() = this(null, null)
  override def getMessage = {
    val message = (underlying, remoteAddress) match {
      case (_, null) => super.getMessage
      case (null, _) => s"ChannelException at remote address: ${remoteAddress.toString}"
      case (_, _) => s"${underlying.getMessage} at remote address: ${remoteAddress.toString}"
    }

    if (serviceName == SourcedException.UnspecifiedServiceName) message
    else s"$message from service: $serviceName"
  }
}

/**
 * Indicates that the client failed to establish a connection. Typically this
 * class will be extended to provide additional information relevant to a
 * particular category of connection failure.
 */
class ConnectionFailedException(underlying: Throwable, remoteAddress: SocketAddress)
  extends ChannelException(underlying, remoteAddress) with NoStacktrace {
  def this() = this(null, null)
}

/**
 * Indicates that a given channel was closed, for instance if the connection
 * was reset by a peer or a proxy.
 */
class ChannelClosedException(underlying: Throwable, remoteAddress: SocketAddress)
  extends ChannelException(underlying, remoteAddress) with NoStacktrace {
  def this(remoteAddress: SocketAddress) = this(null, remoteAddress)
  def this() = this(null, null)
}

/**
 * Indicates that a write to a given `remoteAddress` timed out. See
 * [[com.twitter.finagle.netty3.channel.WriteCompletionTimeoutHandler]] for details.
 */
class WriteTimedOutException(remoteAddress: SocketAddress) extends ChannelException(null, remoteAddress) {
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
 * Marker trait to indicate there was an exception while writing the request.
 * These exceptions should generally be retryable as the full request should
 * not have reached the other end.
 */
trait WriteException extends Exception with SourcedException

/**
 * Default implementation for WriteException that wraps an underlying exception.
 */
case class ChannelWriteException(underlying: Throwable)
  extends ChannelException(underlying)
  with WriteException
  with NoStacktrace
{
  override def fillInStackTrace = this
  override def getStackTrace = underlying.getStackTrace
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

@deprecated("no longer used by com.twitter.finagle.service.FailFastFactory", "7.0.0")
case class FailFastException() extends ChannelException

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

@deprecated("no longer used", "7.0.0")
class ApplicationException extends Exception

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

@deprecated("no longer used", "7.0.0")
class InvalidPipelineException extends ApiException

@deprecated("no longer used", "7.0.0")
class NotYetConnectedException extends ApiException

@deprecated("no longer used", "7.0.0")
class CodecException(description: String) extends Exception(description)

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
object BackupRequestLost extends Exception with NoStacktrace with HasLogLevel {
  def logLevel: Level = Level.TRACE
}
