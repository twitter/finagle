package com.twitter.finagle

// Request failures (eg. for request behavior changing brokers.)
class RequestException             extends Exception
class TimedoutRequestException     extends RequestException
class RetryFailureException        extends RequestException
class CancelledRequestException    extends RequestException
class TooManyWaitersException      extends RequestException
class CancelledConnectionException extends RequestException
class NoBrokersAvailableException  extends RequestException
class ReplyCastException           extends RequestException

abstract class NotServableException extends RequestException
class NotShardableException         extends NotServableException
class ShardNotAvailableException    extends NotServableException

// Channel exceptions are failures on the channels themselves.
class ChannelException                      extends Exception
class ConnectionFailedException             extends ChannelException
class ChannelClosedException                extends ChannelException
class SpuriousMessageException              extends ChannelException
class IllegalMessageException               extends ChannelException
class WriteTimedOutException                extends ChannelException
class UnknownChannelException(e: Throwable) extends ChannelException {
  override def toString = "%s: %s".format(super.toString, e.toString)
}
class WriteException(e: Throwable)          extends ChannelException {
  override def toString = "%s: %s".format(super.toString, e.toString)
}

object ChannelException {
  def apply(cause: Throwable) = {
    cause match {
      case exc: ChannelException => exc
      case _: java.net.ConnectException                    => new ConnectionFailedException
      case _: java.nio.channels.UnresolvedAddressException => new ConnectionFailedException
      case _: java.nio.channels.ClosedChannelException     => new ChannelClosedException
      case e: java.io.IOException if "Connection reset by peer" == e.getMessage =>
        new ChannelClosedException
      case e                                               => new UnknownChannelException(e)
    }
  }
}

// Service layer errors.
class ServiceException             extends Exception
class ServiceClosedException       extends ServiceException
class ServiceNotAvailableException extends ServiceException

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
