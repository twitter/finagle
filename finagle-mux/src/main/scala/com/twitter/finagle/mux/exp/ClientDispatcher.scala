package com.twitter.finagle.mux.exp

import com.twitter.concurrent.{Spool, SpoolSource}
import com.twitter.finagle.{Context, Dtab, NoStacktrace, WriteException}
import com.twitter.finagle.mux._
import com.twitter.finagle.netty3.{BufChannelBuffer, ChannelBufferBuf}
import com.twitter.finagle.tracing.{Trace, Annotation, TraceId}
import com.twitter.finagle.transport.Transport
import com.twitter.io.Buf
import com.twitter.util.{Closable, Future, Local, Promise, Return, Throw, Time, Try, Var}
import java.net.SocketAddress
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference
import java.util.logging.{Level, Logger}
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import scala.collection.JavaConverters._

case class ClientApplicationError(what: String)
  extends Exception(what)
  with NoStacktrace


object ClientDispatcher {
  object ExhaustedTagsException extends Exception("Exhausted tags")
    with WriteException with NoStacktrace

  private[exp] object Remote {
    sealed trait State
    object Active extends State
    case class Draining(acked: Future[Unit]) extends State
  }
}

/**
  * A ClientDispatcher for a mux Session.
  */
class ClientDispatcher private[exp](
  trans: Transport[ChannelBuffer, ChannelBuffer]
) extends MuxService {
  import Message._
  import ClientDispatcher._
  import Spool.*::

  private[this] val log = Logger.getLogger(getClass.getName)
  private[this] val tags = TagSet()
  private[this] val sent = TagMap[SpoolSource[Buf]](tags)
  private[exp] val remoteState = new AtomicReference[Remote.State](Remote.Active)

  /**
   * Dispatches a one-way message to the remote endpoint.
   */
  def send(buf: Buf): Future[Unit] = {
    val contexts = Context.emit().map({ case (k, v) =>
      (BufChannelBuffer(k), BufChannelBuffer(v))
    }).toSeq
    trans.write(encode(Tdispatch(MarkerTag, contexts, "", Dtab.local, BufChannelBuffer(buf))))
  }

  /**
   * Sends a ping to the remote endpoint.
   * Returns a Future[Unit] which will be fulfilled when the remote
   * endpoint acknowledges receiving the ping.
   */
  def ping(): Future[Unit] = {
    val p = new SpoolSource[Buf]
    sent.map(p) match {
      case None =>
        Future.exception(ExhaustedTagsException)
      case Some(tag) =>
        trans.write(encode(Tping(tag))) transform {
          case Throw(t) =>
            sent.unmap(tag)
            Future.exception(t)
          case Return(_) =>
            p.closed
        }
    }
  }

  /**
   * Sends a message to the remote endpoint indicating that the local
   * endpoint will begin nacking dispatches.
   */
  def drain(): Future[Unit] = {
    val p = new SpoolSource[Buf]
    if (remoteState.compareAndSet(Remote.Active, Remote.Draining(p.closed))) {
      sent.map(p) match {
        case None =>
          Future.exception(ExhaustedTagsException)
        case Some(tag) =>
          trans.write(encode(Tdrain(tag))) transform {
            case Throw(t) =>
              sent.unmap(tag)
              Future.exception(t)
            case Return(_) =>
              p.closed
          }
      }
    } else {
      val Remote.Draining(acked) = remoteState.get()
      acked
    }
  }

  override def isAvailable = trans.isOpen

  /**
   * Returns true if drain() has been acknowledged by the remote
   * endpoint. Returns false otherwise.
   */
  private[exp] def isDraining(): Boolean = remoteState.get() match {
    case Remote.Draining(acked) => acked.isDefined
    case _ => false
  }

  /**
   * Notify the other party we are closing.
   */
  override def close(deadline: Time): Future[Unit] = drain()

  /**
   * Transmits a sequenced request and receives a sequenced response.
   */
  def apply(req: Spool[Buf]): Future[Spool[Buf]] = {
    if (req.isEmpty)
      throw new IllegalArgumentException("Cannot dispatch an empty spooled request")

    val rspDiscarded = new Promise[Throwable]
    val rspSource = new SpoolSource[Buf]({ case exc => rspDiscarded.updateIfEmpty(Return(exc)) })
    val tag = sent.map(rspSource) getOrElse {
      return Future.exception(WriteException(new Exception("Exhausted tags")))
    }
    val rsp = rspSource()

    val contexts = Context.emit().map({ case (k, v) =>
      (BufChannelBuffer(k), BufChannelBuffer(v))
    }).toSeq
    val dtab = Dtab.local
    val traceId = Some(Trace.id)

    def terminal(content: Buf): ChannelBuffer =
      encode(Tdispatch(tag, contexts, "", dtab, BufChannelBuffer(content)))

    def continue(content: Buf): ChannelBuffer =
      encode(Tdispatch(tag | TagMSB, contexts, "", dtab, BufChannelBuffer(content)))

    // response is fulfilled as soon as the head is written to the transport
    def writeSpool(s: Spool[Buf]): Future[Unit] = s match {
      case Spool.Empty =>
        Trace.record(Annotation.ClientSend())
        trans.write(terminal(Buf.Empty))

      case buf *:: Future(Return(Spool.Empty)) =>
        Trace.record(Annotation.ClientSend())
        trans.write(terminal(buf))

      case buf *:: Future(Return(tail)) =>
        Trace.record(Annotation.ClientSendFragment())
        trans.write(continue(buf)) onSuccess { _ =>
          writeSpool(tail)
        }

      case buf *:: deferred =>
        Trace.record(Annotation.ClientSendFragment())
        trans.write(continue(buf)) onSuccess { _ =>
          deferred respond {
            case Return(tail) =>
              writeSpool(tail)
            case Throw(exc) =>
              log.log(Level.WARNING, "Deferred tail or request spool resulted in an exception.", exc)
              // Note: we abuse Tdiscarded here to signal cancellation
              // of the streaming request.  A future revision of the
              // protocol should deal with this by adding a status
              // byte to Tdispatch.
              rspDiscarded.updateIfEmpty(Return(ClientApplicationError(exc.toString)))
          }
        }
    }

    // Trace each fragment in the response sequence
    def traceRecv(s: Spool[Buf]): Unit = s match {
      case _ *:: Future(Return(Spool.Empty)) =>
        Trace.record(Annotation.ClientRecv())

      case _ *:: Future(Return(tail)) =>
        Trace.record(Annotation.ClientRecvFragment())
        traceRecv(tail)

      case _ *:: deferred =>
        Trace.record(Annotation.ClientRecvFragment())
        deferred onSuccess { traceRecv(_) }

      case _ => ()
    }

    writeSpool(req) before {
      // Once we've written the first message, send a Tdiscarded if an
      // exception is raised on the response.
      rspDiscarded onSuccess { case exc =>
        sent.maybeRemap(tag, new SpoolSource[Buf]) match {
          case Some(source) =>
            trans.write(encode(Tdiscarded(tag, exc.toString)))
            source.raise(exc)
          case None => ()
        }
      }

      rsp.onSuccess(traceRecv)
    }

    rsp
  }

  private[mux] val recv: Message => Unit = {
    case RdispatchOk(tag, _, rsp) if (tag & TagMSB) > 0 =>
      for (p <- sent.get(tag & MaxTag))
        p.offer(ChannelBufferBuf.Unsafe(rsp))

    case RdispatchOk(tag, _, rsp) =>
      for (p <- sent.unmap(tag))
        p.offerAndClose(ChannelBufferBuf.Unsafe(rsp))

    case RdispatchError(tag, _, error) =>
      for (p <- sent.unmap(tag))
        p.raise(ServerApplicationError(error))

    case RdispatchNack(tag, _) =>
      for (p <- sent.unmap(tag))
        p.raise(RequestNackedException)

    case Rping(tag) =>
      for (p <- sent.unmap(tag))
        p.offerAndClose(Buf.Empty)

    case Rdrain(tag) =>
      for (p <- sent.unmap(tag))
        p.offerAndClose(Buf.Empty)

    case rmsg =>
      log.warning("Did not understand Rmessage[tag=%d] %s".format(rmsg.tag, rmsg))
  }

  private[finagle] def failSent(cause: Throwable) {
    for ((tag, p) <- sent)
      p.raise(cause)
  }
}
