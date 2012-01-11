package com.twitter.finagle

import java.net.SocketAddress

import com.twitter.util.Duration

trait NoStacktrace extends Exception {
  override def fillInStackTrace = this
  // specs expects non-empty stacktrace array
  this.setStackTrace(Array(new StackTraceElement("com.twitter.finagle", "NoStacktrace", null, -1)))
}

// Request failures (eg. for request behavior changing brokers.)
class RequestException(cause: Throwable) extends Exception(cause) with NoStacktrace {
  def this() = this(null)
  override def getStackTrace = if (cause != null) cause.getStackTrace else super.getStackTrace
}

trait TimeoutException { self: Exception =>
  val serviceName: String
  protected val timeout: Duration
  protected val explanation: String

  override def getMessage = "exceeded %s while %s (%s)".format(timeout, explanation, serviceName)
}

class RequestTimeoutException(
  val serviceName: String,
  protected val timeout: Duration,
  protected val explanation: String
) extends RequestException with TimeoutException
class IndividualRequestTimeoutException(serviceName: String, timeout: Duration)
  extends RequestTimeoutException(
    serviceName,
    timeout,
    "waiting for a response for an individual request, excluding retries")
class GlobalRequestTimeoutException(serviceName: String, timeout: Duration)
  extends RequestTimeoutException(
    serviceName,
    timeout,
    "waiting for a response for the request, including retries (if applicable)")

class RetryFailureException(cause: Throwable)        extends RequestException(cause)
class CancelledRequestException                      extends RequestException
class TooManyWaitersException                        extends RequestException
class CancelledConnectionException                   extends RequestException
class NoBrokersAvailableException                    extends RequestException
class ReplyCastException                             extends RequestException

class NotServableException          extends RequestException
class NotShardableException         extends NotServableException
class ShardNotAvailableException    extends NotServableException

// Channel exceptions are failures on the channels themselves.
class ChannelException(underlying: Throwable, val remoteAddress: SocketAddress) extends Exception(underlying) {
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

case class WriteException(underlying: Throwable) extends ChannelException(underlying) with NoStacktrace {
  def this() = this(null)
  override def fillInStackTrace = this
  override def getStackTrace = underlying.getStackTrace
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

// Service layer errors.
class ServiceException                                         extends Exception
class ServiceClosedException                                   extends ServiceException
class ServiceNotAvailableException                             extends ServiceException
class ServiceTimeoutException(
    val serviceName: String,
    protected val timeout: Duration)
    extends ServiceException
    with TimeoutException {
  protected val explanation =
    "creating a service/connection or reserving a service/connection from the service/connection pool"
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
