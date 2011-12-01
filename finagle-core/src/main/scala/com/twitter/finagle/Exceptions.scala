package com.twitter.finagle

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
class ChannelException            (underlying: Throwable) extends Exception(underlying)
class ConnectionFailedException   (underlying: Throwable) extends ChannelException(underlying) with NoStacktrace
class ChannelClosedException      (underlying: Throwable) extends ChannelException(underlying) with NoStacktrace {
  def this() = this(null)
}
class SpuriousMessageException    (underlying: Throwable) extends ChannelException(underlying)
class IllegalMessageException     (underlying: Throwable) extends ChannelException(underlying)
class WriteTimedOutException extends ChannelException(null)
class InconsistentStateException extends ChannelException(null)
case class UnknownChannelException(underlying: Throwable) extends ChannelException(underlying)
case class WriteException (underlying: Throwable) extends ChannelException(underlying) with NoStacktrace {
  override def fillInStackTrace = this
  override def getStackTrace = underlying.getStackTrace
}
case class SslHandshakeException  (underlying: Throwable)              extends ChannelException(underlying)
case class SslHostVerificationException(principal: String)             extends ChannelException(null)
case class ConnectionRefusedException extends ChannelException(
  new Exception("Connection refused by IdleConnectionFilter")
)


object ChannelException {
  def apply(cause: Throwable) = {
    cause match {
      case exc: ChannelException => exc
      case _: java.net.ConnectException                    => new ConnectionFailedException(cause)
      case _: java.nio.channels.UnresolvedAddressException => new ConnectionFailedException(cause)
      case _: java.nio.channels.ClosedChannelException     => new ChannelClosedException(cause)
      case e: java.io.IOException
        if "Connection reset by peer" == e.getMessage      => new ChannelClosedException(cause)
      case e                                               => new UnknownChannelException(cause)
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
