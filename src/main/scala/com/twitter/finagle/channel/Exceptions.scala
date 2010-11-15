package com.twitter.finagle.channel

// Request failures
class RequestException         extends Exception
class TimedoutRequestException extends RequestException
class RetryFailureException    extends RequestException
class ChannelClosedException   extends RequestException

// Subclass this for application exceptions
class ApplicationException extends Exception

class CancelledConnectionException extends Exception

// API misuse errors.
class DiscoException extends Exception
class TooManyDicksOnTheDanceFloorException extends DiscoException
class TooFewDicksOnTheDanceFloorException  extends DiscoException
class InvalidPipelineException extends Exception


// XXXTODO: broker for implementing healthfailure policies.
// (healthcheckingbroker).  can be layered on top of loadedbroker.
