package com.twitter.finagle

import com.twitter.util.Duration
import java.net.SocketAddress

trait SourcedException extends Exception {
  var serviceName: String = "unspecified"
}

trait NoStacktrace extends Exception {
  override def fillInStackTrace = this
  // specs expects non-empty stacktrace array
  this.setStackTrace(Array(new StackTraceElement("com.twitter.finagle", "NoStacktrace", null, -1)))
}

/** Request failures (eg. for request behavior changing brokers.) */
class RequestException(cause: Throwable) extends Exception(cause) with NoStacktrace with SourcedException {
  def this() = this(null)
  override def getStackTrace = if (cause != null) cause.getStackTrace else super.getStackTrace
}

trait TimeoutException extends SourcedException { self: Exception =>
  protected val timeout: Duration
  protected val explanation: String

  override def getMessage = "exceeded %s to %s while %s".format(timeout, serviceName, explanation)
}

class RequestTimeoutException(
  protected val timeout: Duration,
  protected val explanation: String
) extends RequestException with TimeoutException
class IndividualRequestTimeoutException(timeout: Duration)
  extends RequestTimeoutException(
    timeout,
    "waiting for a response for an individual request, excluding retries")
class GlobalRequestTimeoutException(timeout: Duration)
  extends RequestTimeoutException(
    timeout,
    "waiting for a response for the request, including retries (if applicable)")

class NoBrokersAvailableException(
  name: String = "unknown"
) extends RequestException {
  override def getMessage = "No hosts are available for client " + name
}

class RetryFailureException(cause: Throwable)        extends RequestException(cause)
class CancelledRequestException                      extends RequestException
class TooManyWaitersException                        extends RequestException
class CancelledConnectionException                   extends RequestException
class ReplyCastException                             extends RequestException
class FailedFastException                            extends RequestException

class NotServableException          extends RequestException
class NotShardableException         extends NotServableException
class ShardNotAvailableException    extends NotServableException

// Channel exceptions are failures on the channels themselves.
class ChannelException(underlying: Throwable, val remoteAddress: SocketAddress)
  extends Exception(underlying) with SourcedException
{
  def this(underlying: Throwable) = this(underlying, null)
  def this() = this(null, null)
  override def getMessage =
    (underlying, remoteAddress) match {
      case (_, null) => super.getMessage
      case (null, _) => "ChannelException at remote address: %s".format(remoteAddress.toString)
      case (_, _) => "%s at remote address: %s".format(underlying.getMessage, remoteAddress.toString)
    }
}

class ConnectionFailedException(underlying: Throwable, remoteAddress: SocketAddress)
  extends ChannelException(underlying, remoteAddress) with NoStacktrace {
  def this() = this(null, null)
}

class ChannelClosedException(underlying: Throwable, remoteAddress: SocketAddress)
  extends ChannelException(underlying, remoteAddress) with NoStacktrace {
  def this(remoteAddress: SocketAddress) = this(null, remoteAddress)
  def this() = this(null, null)
}

class WriteTimedOutException(remoteAddress: SocketAddress) extends ChannelException(null, remoteAddress) {
  def this() = this(null)
}
class InconsistentStateException(remoteAddress: SocketAddress) extends ChannelException(null, remoteAddress) {
  def this() = this(null)
}

case class UnknownChannelException(underlying: Throwable, override val remoteAddress: SocketAddress)
  extends ChannelException(underlying, remoteAddress) {
  def this() = this(null, null)
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

object WriteException {
  def apply(underlying: Throwable): WriteException =
    ChannelWriteException(underlying)

  def unapply(t: Throwable): Option[Throwable] = t match {
    case we: WriteException => Some(we.getCause)
    case _ => None
  }
}

case class SslHandshakeException(underlying: Throwable, override val remoteAddress: SocketAddress)
  extends ChannelException(underlying, remoteAddress) {
  def this() = this(null, null)
}

case class SslHostVerificationException(principal: String) extends ChannelException {
  def this() = this(null)
}

case class ConnectionRefusedException(override val remoteAddress: SocketAddress)
  extends ChannelException(null, remoteAddress) {
  def this() = this(null)
}

case class RefusedByRateLimiter()          extends ChannelException
case class FailFastException()             extends ChannelException

object ChannelException {
  def apply(cause: Throwable, remoteAddress: SocketAddress) = {
    cause match {
      case exc: ChannelException => exc
      case _: java.net.ConnectException                    => new ConnectionFailedException(cause, remoteAddress)
      case _: java.nio.channels.UnresolvedAddressException => new ConnectionFailedException(cause, remoteAddress)
      case _: java.nio.channels.ClosedChannelException     => new ChannelClosedException(cause, remoteAddress)
      case e: java.io.IOException
        if "Connection reset by peer" == e.getMessage      => new ChannelClosedException(cause, remoteAddress)
      case e                                               => new UnknownChannelException(cause, remoteAddress)
    }
  }
}

// Transport layer errors
class TransportException extends Exception with SourcedException
class CancelledReadException extends TransportException
class CancelledWriteException extends TransportException

// Service layer errors.
trait ServiceException                                         extends Exception with SourcedException
class ServiceClosedException                                   extends ServiceException
class ServiceNotAvailableException                             extends ServiceException

/**
 * Indicates that the connection was not established within the timeouts.
 * This type of exception should generally be safe to retry.
 */
class ServiceTimeoutException(
    override protected val timeout: Duration)
  extends WriteException
  with ServiceException
  with TimeoutException
{
  override protected val explanation =
    "creating a service/connection or reserving a service/connection from the service/connection pool " + serviceName
}

// Subclass this for application exceptions
class ApplicationException extends Exception

// API misuse errors.
class ApiException                         extends Exception
class TooManyConcurrentRequestsException   extends ApiException
class InvalidPipelineException             extends ApiException
class NotYetConnectedException             extends ApiException

class CodecException(description: String) extends Exception(description)

// Channel buffer usage errors.
class ChannelBufferUsageException(description: String) extends Exception(description)

object BackupRequestLost extends Exception with NoStacktrace
