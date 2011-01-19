package com.twitter.finagle

// Request failures (eg. for request behavior changing brokers.)
class RequestException            extends Exception
class TimedoutRequestException    extends RequestException
class RetryFailureException       extends RequestException
class CancelledRequestException   extends RequestException
class NoBrokersAvailableException extends RequestException
class ReplyCastException          extends RequestException

// Channel exceptions are failures on the channels themselves.
class ChannelException                      extends Exception
class ConnectionFailedException             extends ChannelException
class ChannelClosedException                extends ChannelException
class SpuriousMessageException              extends ChannelException
class IllegalMessageException               extends ChannelException
class UnknownChannelException(e: Throwable) extends ChannelException
class WriteException(e: Throwable)          extends ChannelException {
  override def toString = "%s: %s".format(super.toString, e.toString)
}


// Subclass this for application exceptions
class ApplicationException extends Exception

// API misuse errors.
class ApiException                         extends Exception
class TooManyConcurrentRequestsException   extends ApiException
class InvalidPipelineException             extends ApiException
class NotYetConnectedException             extends ApiException
