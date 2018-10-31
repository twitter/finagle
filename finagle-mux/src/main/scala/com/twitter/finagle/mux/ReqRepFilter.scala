package com.twitter.finagle.mux

import com.twitter.finagle.Failure
import com.twitter.finagle.mux.transport.{Message, MuxFailure}
import com.twitter.util.{Return, Throw, Try}

private[finagle] object ReqRepFilter {

  def reply(msg: Message): Try[Response] = msg match {
    case Message.RreqOk(_, rep) =>
      Return(Response(Nil, rep))

    case Message.RreqError(_, error) =>
      Throw(ServerApplicationError(error))

    case r @ Message.RdispatchOk(_, _, rep) =>
      Return(Response(ReqRepHeaders.responseHeaders(r), rep))

    case Message.RdispatchError(_, contexts, error) =>
      val appError = ServerApplicationError(error)
      val exn = MuxFailure.fromContexts(contexts) match {
        case Some(f) => Failure(appError, f.finagleFlags)
        case None => appError
      }
      Throw(exn)

    case Message.RdispatchNack(_, contexts) =>
      val exn = MuxFailure.fromContexts(contexts) match {
        case Some(f) => Failure.RetryableNackFailure.withFlags(f.finagleFlags)
        case None => Failure.RetryableNackFailure
      }
      Throw(exn)

    case Message.RreqNack(_) =>
      Throw(Failure.RetryableNackFailure)

    case m => Throw(Failure(s"unexpected response: $m"))
  }

  /** Indicates if our peer can accept `Tdispatch` messages. */
  object CanDispatch extends Enumeration {
    type State = Value
    val Unknown, Yes, No = Value
  }
}
