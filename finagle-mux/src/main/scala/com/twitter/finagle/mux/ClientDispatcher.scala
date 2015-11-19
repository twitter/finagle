package com.twitter.finagle.mux

import com.twitter.finagle.context.Contexts
import com.twitter.finagle.mux.transport.Message
import com.twitter.finagle.mux.util.{TagMap, TagSet}
import com.twitter.finagle.netty3.{BufChannelBuffer, ChannelBufferBuf}
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.tracing.Trace
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.{Dtab, Filter, Failure, NoStacktrace, Service, ServiceProxy, Status}
import com.twitter.util.{Future, Promise, Return, Throw, Time, Try, Updatable}

/**
 * Indicates that the server failed to interpret or act on the request. This
 * could mean that the client sent a [[com.twitter.finagle.mux]] message type
 * that the server is unable to process.
 */
case class ServerError(what: String)
  extends Exception(what)
  with NoStacktrace

/**
 * Indicates that the server encountered an error whilst processing the client's
 * request. In contrast to [[com.twitter.finagle.mux.ServerError]], a
 * ServerApplicationError relates to server application failure rather than
 * failure to interpret the request.
 */
case class ServerApplicationError(what: String)
  extends Exception(what)
  with NoStacktrace

/**
 * Implements a dispatcher for a mux client. The dispatcher implements the bookkeeping
 * and transactions for outstanding messages and as such exposes an interface where
 * tag assignment can be deferred (i.e. Int => Message).
 */
private[twitter] class ClientDispatcher(trans: Transport[Message, Message])
  extends Service[Int => Message, Message] {
  import ClientDispatcher._

  // outstanding messages, each tagged with a unique int
  // between `MinTag` and `MaxTag`.
  private[this] val tags = TagSet(Message.MinTag to Message.MaxTag)
  private[this] val messages = TagMap[Updatable[Try[Message]]](tags)

  /**
   * Reads indefinitely from the transport, handing successfully
   * decoding messages to `processAndRead`.
   */
  private[this] def readLoop(): Future[Unit] =
    trans.read().flatMap(processAndRead)

  private[this] def processAndRead(msg: Message): Future[Unit] = {
    process(msg)
    readLoop()
  }

  readLoop().onFailure { case exc: Throwable =>
    trans.close()
    val result = Throw(exc)
    for (tag <- tags) {
      // unmap the `tag` here to prevent the associated promise from
      // being fetched from the tag map again, and setting a value twice.
      for (u <- messages.unmap(tag))
        u() = result
    }
  }

  private[this] def process(msg: Message): Unit = msg match {
    case Message.Rerr(_, err) =>
      for (u <- messages.unmap(msg.tag))
        u() = Throw(ServerError(err))

    case Message.Rmessage(_) =>
      for (u <- messages.unmap(msg.tag))
        u() = Return(msg)

    case _ => // do nothing.
  }

  /**
   * Dispatch the message resulting from applying `f`
   * with a fresh tag.
   */
  def apply(f: Int => Message): Future[Message] = {
    val p = new Promise[Message]
    messages.map(p) match {
      case None => FutureExhaustedTagsException
      case Some(tag) =>
        val msg = f(tag)
        trans.write(msg).transform {
          case Return(_) =>
            p.setInterruptHandler { case cause =>
              // We replace the current Updatable, if any, with a stand-in to reserve
              // the tag of discarded requests until Tdiscarded is acknowledged by the
              // peer.
              for (u <- messages.maybeRemap(msg.tag, Empty)) {
                trans.write(Message.Tdiscarded(msg.tag, cause.toString))
                u() = Throw(cause)
              }
            }
            p
          case t@Throw(_) =>
            messages.unmap(tag)
            Future.const(t.cast[Message])
        }
    }
  }

  override def status: Status = trans.status
  override def close(when: Time): Future[Unit] = trans.close(when)
}

private[twitter] object ClientDispatcher {
  val FutureExhaustedTagsException = Future.exception(
    Failure.rejected("Exhausted tags"))

  val Empty: Updatable[Try[Message]] = Updatable.empty()

  /**
   * Creates a mux client dispatcher that can handle mux Request/Responses.
   */
  def newRequestResponse(trans: Transport[Message, Message]): Service[Request, Response] =
    new ReqRepFilter andThen new ClientDispatcher(trans)
}

/**
 * Bridges a mux ClientDispatcher to mux.Requests/mux.Responses. This includes support
 * for `Tdispatch` while downgrading to `Treq` if neccessary.
 */
private class ReqRepFilter extends Filter[Request, Response, Int => Message, Message] {
  import ReqRepFilter._

  // We currently attempt a Tdispatch and if it fails set canDispatch to No.
  // When Tinits/Rinits arrive, we should use them to signal this capability
  // instead.
  @volatile private[this] var canDispatch: CanDispatch.State = CanDispatch.Unknown

  private[this] def reply(msg: Try[Message]): Future[Response] = msg match {
      case Return(Message.RreqOk(_, rep)) =>
        Future.value(Response(ChannelBufferBuf.Owned(rep)))

      case Return(Message.RreqError(_, error)) =>
        Future.exception(ServerApplicationError(error))

      case Return(Message.RdispatchOk(_, _, rep)) =>
        Future.value(Response(ChannelBufferBuf.Owned(rep)))

     case Return(Message.RdispatchError(_, _, error)) =>
        Future.exception(ServerApplicationError(error))

      case Return(Message.RreqNack(_)) | Return(Message.RdispatchNack(_, _))  =>
        FutureNackedException

      case t@Throw(_) => Future.const(t.cast[Response])
      case Return(m) => Future.exception(Failure(s"unexpected response: $m"))
  }

  def apply(req: Request, svc: Service[Int => Message, Message]): Future[Response] = {
    val couldDispatch = canDispatch

    val msg = couldDispatch match {
      case CanDispatch.No => { tag: Int =>
        Message.Treq(tag, Some(Trace.id), BufChannelBuffer(req.body))
      }

      case CanDispatch.Yes | CanDispatch.Unknown => { tag: Int =>
        val contexts = Contexts.broadcast.marshal().map {
          case (k, v) => (BufChannelBuffer(k), BufChannelBuffer(v))
        }
        Message.Tdispatch(tag, contexts.toSeq, req.destination, Dtab.local,
          BufChannelBuffer(req.body))
      }
    }

    if (couldDispatch != CanDispatch.Unknown) svc(msg).transform(reply)
    else svc(msg).transform {
      case Throw(ServerError(_)) =>
        // We've determined that the server cannot handle Tdispatch messages,
        // so we fall back to a Treq.
        canDispatch = CanDispatch.No
        apply(req, svc)

      case r@Return(_) =>
        canDispatch = CanDispatch.Yes
        reply(r)

      case t@Throw(_) => reply(t)
    }
  }
}

private object ReqRepFilter {
  val FutureNackedException = Future.exception(
    Failure.rejected("The request was Nacked by the server"))

  /** Indicates if our peer can accept `Tdispatch` messages. */
  object CanDispatch extends Enumeration {
    type State = Value
    val Unknown, Yes, No = Value
  }
}