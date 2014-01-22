package com.twitter.finagle
package mux

import com.twitter.concurrent.{Offer, Broker}
import com.twitter.finagle.netty3.ChannelBufferBuf
import com.twitter.finagle.tracing.{Trace, Annotation, TraceId}
import com.twitter.finagle.transport.Transport
import com.twitter.io.Buf
import com.twitter.util.{Closable, Future, Local, Promise, Return, Throw, Time, Try, Var}
import java.net.SocketAddress
import java.util.concurrent.ConcurrentHashMap
import java.util.logging.Logger
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import scala.collection.JavaConverters._

private sealed trait TransportState
private case object Open extends TransportState
private case class Closing(deadline: Time) extends TransportState
private case object Closed extends TransportState

/**
 * An interface representing the RPC, message-passing, pinging, and
 * draining facilities provided by a mux session.
 */
trait Session extends Service[Buf, Buf] with Closable {
  private[mux] val transState = Var[TransportState](Open)
  private[this] val pClosed = new Promise[Unit]

  transState observe {
    case Closed => pClosed.setDone()
    case _ => ()
  }

  def message(buf: Buf): Future[Unit]
  def ping(): Future[Unit]
  def drain(): Future[Unit]
  override def close(deadline: Time): Future[Unit] = {
    transState.update(Closing(deadline))
    pClosed
  }

  val onClose: Future[Unit] = pClosed
}

object ExhaustedTagsException
  extends Exception("Exhausted tags")
  with WriteException with NoStacktrace


private sealed trait DrainState
private case object Active extends DrainState
private case object Draining extends DrainState

/*
 * TODO: figure out what kind of Dtab semantics make sense for
 * message passing.
 */

class SenderSession private[finagle](
  trans: Transport[ChannelBuffer, ChannelBuffer]
) extends Session with Closable {
  import Message._

  private[this] val log = Logger.getLogger(getClass.getName)
  private[this] val tags = TagSet()
  private[this] val sent = TagMap[Promise[Buf]](tags)
  @volatile private[this] var canDispatch: Cap.State = Cap.Unknown
  private[mux] val drainState = Var[DrainState](Active)

  def apply(buf: Buf): Future[Buf] = dispatchRpc(ToCB(buf), true)

  private def dispatchRpc(
    req: ChannelBuffer,
    traceWrite: Boolean
  ): Future[Buf] = {
    val p = new Promise[Buf]
    val tag = sent.map(p) getOrElse {
      return Future.exception(WriteException(new Exception("Exhausted tags")))
    }

    val couldDispatch = canDispatch
    val msg = if (couldDispatch == Cap.No) Treq(tag, Some(Trace.id), req) else {
      val contexts = Context.emit() map { case (k, v) => (ToCB(k), ToCB(v)) }
      Tdispatch(tag, contexts.toSeq, "", Dtab.empty, req)
    }

    if (traceWrite)
      Trace.record(Annotation.ClientSend())

    trans.write(encode(msg)) onFailure { case exc =>
      sent.unmap(tag)
    } before {
      p.setInterruptHandler { case cause =>
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
      p onSuccess { _ =>
        canDispatch = Cap.Yes
      } rescue {
        case ServerError(_) =>
          canDispatch = Cap.No
          dispatchRpc(req, false)
      }
    } else p
  }

  def ping(): Future[Unit] = {
    val p = new Promise[Buf]
    val tag = sent.map(p) getOrElse {
      return Future.exception(WriteException(new Exception("Exhausted tags")))
    }
    trans.write(encode(Tping(tag))) onFailure { case exc =>
      sent.unmap(tag)
    } flatMap(Function.const(p)) map(Function.const(()))
  }

  def drain(): Future[Unit] = {
    drainState.update(Draining)
    val p = new Promise[Buf]
    val tag = sent.map(p) getOrElse {
      return Future.exception(WriteException(new Exception("Exhausted tags")))
    }
    trans.write(encode(Tdrain(tag))) onFailure { case exc =>
      sent.unmap(tag)
    } flatMap(Function.const(p)) map(Function.const(()))
  }

  def message(buf: Buf): Future[Unit] =
    trans.write(encode(Treq(MarkerTag, None, ToCB(buf))))

  private[finagle] val recv: Message => Unit = {
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

/**
 * SessionDispatcher repeatedly calls recv() on transort, and dispatches the messages
 * to the appropriate Session instance.
 */
class SessionDispatcher private[finagle](
  trans: Transport[ChannelBuffer, ChannelBuffer],
  sender: SenderSession,
  receiver: Session
) {
  import Message._

  private[this] val log = Logger.getLogger(getClass.getName)
  private[this] val pending = new ConcurrentHashMap[Int, Future[_]]
  private[this] val transState: Var[TransportState] = for {
    s <- sender.transState
    r <- receiver.transState
  } yield {
    (s, r) match {
      case (Open, Open) => Open
      case (Closing(d1), Closing(d2)) => Closing(d1 min d2)
      case (Closing(d), _) => Closing(d)
      case (_, Closing(d)) => Closing(d)
      case _ => Closed
    }
  }

  transState observe {
    case Closing(d) => close(d)
    case _ => ()
  }

  trans.onClose respond { _ =>
    sender.transState.update(Closed)
    receiver.transState.update(Closed)
  }

  def dispatch(message: Message) {
    message match {
      case Treq(0, None, buf) =>
        receiver.message(ChannelBufferBuf(buf))

      case Tdispatch(tag, contexts, _dst, _dtab, req) =>
        if (sender.drainState() == Draining) {
          trans.write(encode(RdispatchNack(tag, Seq.empty)))
          return
        }
        val save = Local.save()
        try {
          for ((k, v) <- contexts)
            Context.handle(ChannelBufferBuf(k), ChannelBufferBuf(v))
          Trace.record(Annotation.ServerRecv())
          val f = receiver(ChannelBufferBuf(req))
          pending.put(tag, f)
          f respond {
            case Return(rsp) =>
              pending.remove(tag)
              Trace.record(Annotation.ServerSend())
              trans.write(encode(RdispatchOk(tag, Seq.empty, ToCB(rsp))))
            case Throw(exc) =>
              trans.write(encode(RdispatchError(tag, Seq.empty, exc.toString)))
          }
        } finally {
          Local.restore(save)
        }

      case Treq(tag, traceId, req) =>
        if (sender.drainState() == Draining) {
          trans.write(encode(RreqNack(tag)))
          return
        }
        val saved = Trace.state
        try {
          for (traceId <- traceId)
            Trace.setId(traceId)
          Trace.record(Annotation.ServerRecv())
          val f = receiver(ChannelBufferBuf(req))
          pending.put(tag, f)
          f respond {
            case Return(rsp) =>
              pending.remove(tag)
              Trace.record(Annotation.ServerSend())
              trans.write(encode(RreqOk(tag, ToCB(rsp))))
            case Throw(exc) =>
              trans.write(encode(RreqError(tag, exc.toString)))
          }
        } finally {
          Trace.state = saved
        }

      case Tdiscarded(tag, why) =>
        pending.get(tag) match {
          case null => ()
          case f => f.raise(new ClientDiscardedRequestException(why))
        }

      case Tping(tag) =>
        if (sender.drainState() == Active) {
          receiver.ping() onSuccess { _ =>
            trans.write(encode(Rping(tag)))
          }
        }

      case Tdrain(tag) =>
        receiver.drain() onSuccess { _ =>
          trans.write(encode(Rdrain(tag)))
        }

      case m@Tmessage(tag) =>
        log.warning("Did not understand Tmessage[tag=%d] %s".format(tag, m))
        trans.write(encode(Rerr(tag, "badmessage")))

      case rmsg@Rmessage(tag) =>
        sender.recv(rmsg)
    }
  }

  private[this] def loop(): Future[Nothing] =
    trans.read() flatMap { buf =>
      try {
        val m = decode(buf)
        dispatch(m)
        loop()
      } catch {
        case exc: BadMessageException =>
          // We could just ignore this message, but in reality it
          // probably means something is really FUBARd.
          Future.exception(exc)
      }
    }

  loop() onFailure { case cause =>
    // We know that if we have a failure, we cannot from this point forward
    // insert new entries in the pending map.
    val exc = ClientHangupException(cause)
    for ((_, f) <- pending.asScala)
      f.raise(exc)
    pending.clear()
    trans.close()
    sender.failSent(cause)
  }

  def close(deadline: Time) = trans.close(deadline)
  def isOpen = trans.isOpen
  def remoteAddress = trans.remoteAddress
  def localAddress = trans.localAddress
}

private[finagle] object ToCB {
  def apply(buf: Buf) =
    buf match {
      case Buf.ByteArray(bytes, begin, end) =>
        ChannelBuffers.wrappedBuffer(bytes, begin, end-begin)
      case buf =>
        val bytes = new Array[Byte](buf.length)
        buf.write(bytes, 0)
        ChannelBuffers.wrappedBuffer(bytes)
    }
}
