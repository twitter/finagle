package com.twitter.finagle.mux

import com.twitter.finagle.{Dtab, FailureFlags, Filter, Path, Service}
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.mux.transport.{Message, MuxFailure}
import com.twitter.finagle.tracing.Trace
import com.twitter.util.{Future, Return, Throw}

/**
 * Processor handles request, dispatch, and ping messages. Request
 * and dispatch messages are passed onto the request-response in the
 * filter chain. Pings are answered immediately in the affirmative.
 *
 * (This arrangement permits interpositioning other filters to modify ping
 * or dispatch behavior, e.g., for testing.)
 */
private[finagle] object ServerProcessor extends Filter[Message, Message, Request, Response] {
  import Message._

  private[this] val AlwaysEmpty = { _: Throwable => MuxFailure.Empty }

  private[this] def dispatch(
    tdispatch: Message.Tdispatch,
    service: Service[Request, Response]
  ): Future[Message] = {

    Contexts.broadcast.letUnmarshal(tdispatch.contexts) {
      if (tdispatch.dtab.nonEmpty)
        Dtab.local ++= tdispatch.dtab

      val result = ReqRepHeaders.withApplicationHeaders { headers =>
        service(Request(tdispatch.dst, headers, tdispatch.req))
      }

      result.transform {
        case Return(rep) =>
          val contexts = ReqRepHeaders.toDispatchContexts(rep).toSeq
          Future.value(RdispatchOk(tdispatch.tag, contexts, rep.body))

        // Previously, all Restartable failures were sent as RdispatchNack
        // messages. In order to keep backwards compatibility with clients that
        // do not look for MuxFailures, this behavior is left alone. additional
        // MuxFailure flags are still sent.
        case Throw(f: FailureFlags[_]) if f.isFlagged(FailureFlags.Retryable) =>
          val mFail = MuxFailure.FromThrow.applyOrElse(f, AlwaysEmpty)
          Future.value(RdispatchNack(tdispatch.tag, mFail.contexts))

        case Throw(exc) =>
          val mFail = MuxFailure.FromThrow.applyOrElse(exc, AlwaysEmpty)
          Future.value(RdispatchError(tdispatch.tag, mFail.contexts, exc.toString))
      }
    }
  }

  private[this] def dispatch(
    treq: Message.Treq,
    service: Service[Request, Response]
  ): Future[Message] = {
    Trace.letIdOption(treq.traceId) {
      service(Request(Path.empty, Nil, treq.req)).transform {
        case Return(rep) =>
          Future.value(RreqOk(treq.tag, rep.body))

        case Throw(f: FailureFlags[_]) if f.isFlagged(FailureFlags.Retryable) =>
          Future.value(Message.RreqNack(treq.tag))

        case Throw(exc) =>
          Future.value(Message.RreqError(treq.tag, exc.toString))
      }
    }
  }

  def apply(req: Message, service: Service[Request, Response]): Future[Message] = req match {
    case d: Message.Tdispatch => dispatch(d, service)
    case r: Message.Treq => dispatch(r, service)
    case Message.Tping(Message.Tags.PingTag) => Message.PreEncoded.FutureRping
    case Message.Tping(tag) => Future.value(Message.Rping(tag))
    case m => Future.exception(new IllegalArgumentException(s"Cannot process message $m"))
  }
}
