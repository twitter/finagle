package com.twitter.finagle.mux

import com.twitter.finagle.tracing.{Trace, Annotation}
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.Context
import com.twitter.finagle.{WriteException, NoStacktrace, Dtab}
import com.twitter.util.{Future, Promise, Time}
import com.twitter.io.Buf
import java.util.logging.Logger
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}

import com.twitter.finagle.Service

object RequestNackedException
  extends Exception("The request was nackd by the server")
  with WriteException with NoStacktrace

case class ServerError(what: String)
  extends Exception(what)
  with NoStacktrace

case class ServerApplicationError(what: String)
  extends Exception(what)
  with NoStacktrace

private object Cap extends Enumeration {
  type State = Value
  val Unknown, Yes, No = Value
}

/**
 * A ClientDispatcher for the mux protocol.
 */
private[finagle] class ClientDispatcher(trans: Transport[ChannelBuffer, ChannelBuffer])
  extends Service[ChannelBuffer, ChannelBuffer]
{
  import Message._
  
  @volatile private[this] var canDispatch: Cap.State = Cap.Unknown

  private[this] val tags = TagSet()
  private[this] val reqs = TagMap[Promise[ChannelBuffer]](tags)
  private[this] val log = Logger.getLogger(getClass.getName)

  private[this] val receive: Message => Unit = {
    case RreqOk(tag, rep) =>
      for (p <- reqs.unmap(tag))
        p.setValue(rep)
    case RreqError(tag, error) =>
      for (p <- reqs.unmap(tag))
        p.setException(ServerApplicationError(error))
    case RreqNack(tag) =>
      for (p <- reqs.unmap(tag))
        p.setException(RequestNackedException)

    case RdispatchOk(tag, _, rep) =>
      for (p <- reqs.unmap(tag))
        p.setValue(rep)
    case RdispatchError(tag, _, error) =>
      for (p <- reqs.unmap(tag))
        p.setException(ServerApplicationError(error))
    case RdispatchNack(tag, _) =>
      for (p <- reqs.unmap(tag))
        p.setException(RequestNackedException)
    
    case Rerr(tag, error) =>
      for (p <- reqs.unmap(tag))
        p.setException(ServerError(error))

    case Rping(tag) =>
      for (p <- reqs.unmap(tag))
        p.setValue(ChannelBuffers.EMPTY_BUFFER)
    case Tping(tag) =>
      trans.write(encode(Rping(tag)))
    case m@Tmessage(tag) =>
      log.warning("Did not understand Tmessage[tag=%d] %s".format(tag, m))
      trans.write(encode(Rerr(tag, "badmessage")))
    case m@Rmessage(tag) =>
      val what = "Did not understand Rmessage[tag=%d] %s".format(tag, m)
      log.warning(what)
      for (p <- reqs.unmap(tag))
        p.setException(BadMessageException(what))
  }

  private[this] val readAndAct: ChannelBuffer => Future[Nothing] =
    buf => try {
      val m = decode(buf)
      receive(m)
      loop()
    } catch {
      case exc: BadMessageException =>
        Future.exception(exc)
    }

  private[this] def loop(): Future[Nothing] =
    trans.read() flatMap readAndAct

  loop() onFailure { case exc =>
    trans.close()
    for ((tag, p) <- reqs)
      p.setException(exc)
  }

  def ping(): Future[Unit] = {
    val p = new Promise[ChannelBuffer]
    reqs.map(p) match {
      case None =>
        Future.exception(WriteException(new Exception("Exhausted tags")))
      case Some(tag) =>
        trans.write(encode(Tping(tag))) onFailure { case exc =>
          reqs.unmap(tag)
        } flatMap(Function.const(p)) map(Function.const(()))
    }
  }

  def apply(req: ChannelBuffer): Future[ChannelBuffer] = dispatch(req, true)
  
  private def toCB(buf: Buf) =
    buf match {
      case Buf.ByteArray(bytes, begin, end) =>
        ChannelBuffers.wrappedBuffer(bytes, begin, end-begin)
      case buf =>
        val bytes = new Array[Byte](buf.length)
        buf.write(bytes, 0)
        ChannelBuffers.wrappedBuffer(bytes)
    }
  
  private def dispatch(
    req: ChannelBuffer, 
    traceWrite: Boolean
  ): Future[ChannelBuffer] = {
    val p = new Promise[ChannelBuffer]
    val tag = reqs.map(p) getOrElse {
      return Future.exception(WriteException(new Exception("Exhausted tags")))
    }

    val couldDispatch = canDispatch
    val msg = if (couldDispatch == Cap.No) Treq(tag, Some(Trace.id), req) else {
      val contexts = Context.emit() map { case (k, v) => (toCB(k), toCB(v)) }
      Tdispatch(tag, contexts.toSeq, "", Dtab.baseDiff(), req)
    }

    if (traceWrite)
      Trace.record(Annotation.ClientSend())

    trans.write(encode(msg)) onFailure { case exc =>
      reqs.unmap(tag)
    } before {
      p.setInterruptHandler { case cause =>
        for (reqP <- reqs.maybeRemap(tag, new Promise[ChannelBuffer])) {
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
          dispatch(req, false)
      }
    } else p
  }

  override def isAvailable = trans.isOpen
  override def close(deadline: Time) = trans.close(deadline)
}
