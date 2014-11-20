package com.twitter.finagle.mux

import com.twitter.conversions.time._
import com.twitter.finagle.mux.lease.Acting
import com.twitter.finagle.netty3.{ChannelBufferBuf, BufChannelBuffer}
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.tracing.Trace
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.{Context, Dtab, Service, WriteException, NoStacktrace}
import com.twitter.util.{Future, Promise, Time, Duration}
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.logging.Logger
import org.jboss.netty.buffer.ChannelBuffer

/**
 * Indicates that a client request was denied by the server.
 */
object RequestNackedException
  extends Exception("The request was nackd by the server")
  with WriteException with NoStacktrace

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

private case class Lease(end: Time) {
  def remaining: Duration = end.sinceNow
  def expired: Boolean = end < Time.now
}

private object Lease {
  import Message.Tlease

  def parse(unit: Byte, howMuch: Long): Option[Lease] = unit match {
    case Tlease.MillisDuration => Some(new Lease(howMuch.milliseconds.fromNow))
    case _ => None
  }
}

private object Cap extends Enumeration {
  type State = Value
  val Unknown, Yes, No = Value
}

/**
 * A ClientDispatcher for the mux protocol.
 */
private[finagle] class ClientDispatcher (
  trans: Transport[ChannelBuffer, ChannelBuffer],
  sr: StatsReceiver
) extends Service[Request, Response] with Acting {
  // protects the "drained" field, so that we don't send a request
  // after sending an Rdrain message
  private[this] val drainLock = new ReentrantReadWriteLock
  private[this] val checkDrained = drainLock.readLock
  private[this] val setDrained = drainLock.writeLock

  import Message._

  @volatile private[this] var canDispatch: Cap.State = Cap.Unknown
  @volatile private[this] var drained = false

  private[this] val futureNackedException = Future.exception(RequestNackedException)
  private[this] val tags = TagSet()
  private[this] val reqs = TagMap[Promise[Response]](tags)
  private[this] val log = Logger.getLogger(getClass.getName)

  @volatile private[this] var lease = new Lease(Time.Top)

  private[this] val gauge = sr.addGauge("current_lease_ms") {
    lease.remaining.inMilliseconds
  }
  private[this] val leaseCounter = sr.counter("lease_counter")

  private[this] val receive: Message => Unit = {
    case RreqOk(tag, rep) =>
      for (p <- reqs.unmap(tag))
        p.setValue(Response(ChannelBufferBuf.Owned(rep)))
    case RreqError(tag, error) =>
      for (p <- reqs.unmap(tag))
        p.setException(ServerApplicationError(error))
    case RreqNack(tag) =>
      for (p <- reqs.unmap(tag))
        p.setException(RequestNackedException)

    case RdispatchOk(tag, _, rep) =>
      for (p <- reqs.unmap(tag))
        p.setValue(Response(ChannelBufferBuf.Owned(rep)))
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
        p.setValue(Response.empty)
    case Tping(tag) =>
      trans.write(encode(Rping(tag)))
    case Tdrain(tag) =>
      // must be synchronized to avoid writing after Rdrain has been sent
      setDrained.lockInterruptibly()
      try {
        drained = true
        trans.write(encode(Rdrain(tag)))
      } finally {
        setDrained.unlock()
      }
    case Tlease(unit, howMuch) =>
      Lease.parse(unit, howMuch) foreach { newLease =>
        log.fine("leased for " + newLease + " to " + trans.remoteAddress)
        leaseCounter.incr()
        lease = newLease
      }
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
    val p = new Promise[Response]
    reqs.map(p) match {
      case None =>
        Future.exception(WriteException(new Exception("Exhausted tags")))
      case Some(tag) =>
        trans.write(encode(Tping(tag))).onFailure { case exc =>
          reqs.unmap(tag)
        }.flatMap(Function.const(p)).unit
    }
  }

  def apply(req: Request): Future[Response] = {
    checkDrained.lockInterruptibly()
    try {
      if (drained)
        futureNackedException
      else
        dispatch(req)
    } finally {
      checkDrained.unlock()
    }
  }

  /**
   * Dispatch a request.
   *
   * @param req the buffer representation of the request to be dispatched
   */
  private def dispatch(req: Request): Future[Response] = {
    val p = new Promise[Response]
    val couldDispatch = canDispatch

    val tag = reqs.map(p) getOrElse {
      return Future.exception(WriteException(new Exception("Exhausted tags")))
    }

    val msg =
      if (couldDispatch == Cap.No)
        Treq(tag, Some(Trace.id), BufChannelBuffer(req.body))
      else {
        val contexts = Context.emit() map { case (k, v) =>
          (BufChannelBuffer(k), BufChannelBuffer(v))
        }
        Tdispatch(tag, contexts.toSeq, req.destination, Dtab.local,
          BufChannelBuffer(req.body))
      }

    trans.write(encode(msg)) onFailure { case exc =>
      reqs.unmap(tag)
    } before {
      p.setInterruptHandler { case cause =>
        for (reqP <- reqs.maybeRemap(tag, new Promise[Response])) {
          trans.write(encode(Tdiscarded(tag, cause.toString)))
          reqP.setException(cause)
        }
      }
      p
    }

    if (couldDispatch == Cap.Unknown) {
      p onSuccess { _ =>
        canDispatch = Cap.Yes
      } rescue {
        case ServerError(_) =>
          // We've determined that the server cannot handle Tdispatch messages,
          // so we fall back to a Treq and disable tracing in order to not
          // double-count the request.
          canDispatch = Cap.No
          dispatch(req)
      }
    } else p
  }

  override def isAvailable: Boolean = trans.isOpen

  def isActive: Boolean = !lease.expired && !drained

  override def close(deadline: Time): Future[Unit] = trans.close(deadline)
}
