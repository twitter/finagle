package com.twitter.finagle.mux.exp

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
import java.util.logging.Logger
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import scala.collection.JavaConverters._

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

  private[this] val log = Logger.getLogger(getClass.getName)
  private[this] val tags = TagSet()
  private[this] val sent = TagMap[Promise[Buf]](tags)
  private[exp] val remoteState = new AtomicReference[Remote.State](Remote.Active)
  @volatile private[this] var canDispatch: Cap.State = Cap.Unknown

  /**
   * Dispatches an RPC to the remote endpoint.
   */
  def apply(buf: Buf): Future[Buf] = dispatchRpc(BufChannelBuffer(buf), true)

  /**
   * Dispatches a one-way message to the remote endpoint.
   */
  def send(buf: Buf): Future[Unit] =
    trans.write(encode(Treq(MarkerTag, None, BufChannelBuffer(buf))))

  /**
   * Sends a ping to the remote endpoint.
   * Returns a Future[Unit] which will be fulfilled when the remote
   * endpoint acknowledges receiving the ping.
   */
  def ping(): Future[Unit] = {
    val p = new Promise[Buf]
    sent.map(p) match {
      case None =>
        Future.exception(ExhaustedTagsException)
      case Some(tag) =>
        trans.write(encode(Tping(tag))) transform {
          case t: Throwable =>
            sent.unmap(tag)
            Future.const(t)
          case Return(_) =>
            p.unit
        }
    }
  }

  /**
   * Sends a message to the remote endpoint indicating that the local
   * endpoint will begin nacking dispatches.
   */
  def drain(): Future[Unit] = {
    val p = new Promise[Buf]
    if (remoteState.compareAndSet(Remote.Active, Remote.Draining(p.unit))) {
      sent.map(p) match {
        case None =>
          Future.exception(ExhaustedTagsException)
        case Some(tag) =>
          trans.write(encode(Tdrain(tag))) transform {
            case t: Throwable =>
              sent.unmap(tag)
              Future.const(t)
            case Return(_) =>
              p.unit
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

  private[this] def dispatchRpc(
    req: ChannelBuffer,
    traceWrite: Boolean
  ): Future[Buf] = {
    val p = new Promise[Buf]
    val tag = sent.map(p) getOrElse {
      return Future.exception(ExhaustedTagsException)
    }

    val couldDispatch = canDispatch
    val msg = if (couldDispatch == Cap.No) {
      Treq(tag, Some(Trace.id), req)
    } else {
      val contexts = Context.emit() map { case (k, v) =>
        (BufChannelBuffer(k), BufChannelBuffer(v))
      }
      Tdispatch(tag, contexts.toSeq, "", Dtab.empty, req)
    }

    if (traceWrite)
      Trace.record(Annotation.ClientSend())

    trans.write(encode(msg)) respond {
      case Throw(exc) =>
        sent.unmap(tag)

      case Return(()) =>
        if (couldDispatch == Cap.Unknown)
          canDispatch = Cap.Yes

        p setInterruptHandler { case cause =>
          for (reqP <- sent.maybeRemap(tag, new Promise[Buf])) {
            trans.write(encode(Tdiscarded(tag, cause.toString)))
            reqP.setException(cause)
          }
        }
        p onSuccess { _ =>
          Trace.record(Annotation.ClientRecv())
        }
    }

    if (couldDispatch == Cap.Unknown) {
      p rescue {
        case ServerError(_) =>
          canDispatch = Cap.No
          dispatchRpc(req, false)
      }
    } else p
  }

  private[mux] val recv: Message => Unit = {
    case RreqOk(tag, cb) =>
      for (p <- sent.unmap(tag))
        p.setValue(ChannelBufferBuf(cb))

    case RreqError(tag, error) =>
      for (p <- sent.unmap(tag))
        p.setException(ServerApplicationError(error))

    case RreqNack(tag) =>
      for (p <- sent.unmap(tag))
        p.setException(RequestNackedException)

    case RdispatchOk(tag, _, rsp) =>
      for (p <- sent.unmap(tag))
        p.setValue(ChannelBufferBuf(rsp))

    case RdispatchError(tag, _, error) =>
      for (p <- sent.unmap(tag))
        p.setException(ServerApplicationError(error))

    case RdispatchNack(tag, _) =>
      for (p <- sent.unmap(tag))
        p.setException(RequestNackedException)

    case Rping(tag) =>
      for (p <- sent.unmap(tag))
        p.setValue(Buf.Empty)

    case Rdrain(tag) =>
      for (p <- sent.unmap(tag))
        p.setValue(Buf.Empty)

    case rmsg =>
      log.warning("Did not understand Rmessage[tag=%d] %s".format(rmsg.tag, rmsg))
  }

  private[finagle] def failSent(cause: Throwable) {
    for ((tag, p) <- sent)
      p.setException(cause)
  }
}
