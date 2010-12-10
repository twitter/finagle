package com.twitter.finagle.channel

// Request failures (eg. for request behavior changing brokers.)
class RequestException          extends Exception
class TimedoutRequestException  extends RequestException
class RetryFailureException     extends RequestException
class CancelledRequestException extends Exception

// Channel exceptions are failures on the channels themselves.
class ChannelException                      extends Exception
class ConnectionFailedException             extends ChannelException
class ChannelClosedException                extends ChannelException
class UnknownChannelException(e: Throwable) extends ChannelException

// Subclass this for application exceptions
class ApplicationException extends Exception

// API misuse errors.
class ApiException                         extends Exception
class TooManyDicksOnTheDanceFloorException extends ApiException
class TooFewDicksOnTheDanceFloorException  extends ApiException
class InvalidPipelineException             extends ApiException
class NotYetConnectedException             extends ApiException
