package com.twitter.finagle.mux

import com.twitter.conversions.time._
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.netty3.{BufChannelBuffer, ChannelBufferBuf}
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.tracing.Trace
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.util.DefaultTimer
import com.twitter.finagle.{Dtab, Failure, NoStacktrace, Service, Status, WriteException}
import com.twitter.util.{Duration, Future, Promise, Return, Throw, Time, Try, Updatable}

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.logging.{Level, Logger}
import org.jboss.netty.buffer.{ChannelBuffer, ReadOnlyChannelBuffer}

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

private object Cap extends Enumeration {
  type State = Value
  val Unknown, Yes, No = Value
}

private[twitter] object ClientDispatcher {

  /**
   * The client dispatcher can be in one of 4 states,
   * independent from its transport.
   *
   *  - [[Dispatching]] is the stable operating state of a Dispatcher.
   *    Requests are dispatched, and the dispatcher's status is
   *    [[com.twitter.finagle.Status.Open]].
   *  - A dispatcher is [[Draining]] when it has received a `Tdrain`
   *    message from its peer, but still has outstanding requests.
   *    In this state, we have promised our peer not to send any more
   *    requests, thus the dispatcher's status is
   *    [[com.twitter.finagle.Status.Busy]], and requests for service are
   *    denied.
   *  - When a dispatcher is fully drained; that is, it has received a
   *    `Tdrain` and there are no more pending requests, the dispatcher's
   *    state is set to [[Drained]]. In this state, no more requests are
   *    admitted, and they never will be. The dispatcher is useless. It is
   *    dead. Its status is set to [[com.twitter.finagle.Status.Closed]].
   *  - Finally, if a server has issued the client a lease, its state is
   *    set to [[Leasing]] which composes the lease expiry time. This state
   *    is equivalent to [[Dispatching]] except if the lease has expired.
   *    At this time, the dispatcher's status is set to
   *    [[com.twitter.finagle.Status.Busy]]; however, leases are advisory
   *    and requests are still admitted.
   */
  sealed trait State
  case object Dispatching extends State
  case object Draining extends State
  case object Drained extends State
  case class Leasing(end: Time) extends State {
    def remaining: Duration = end.sinceNow
    def expired: Boolean = end < Time.now
  }

  // We reserve a tag for a default ping message to that we
  // can cache a full ping message and avoid encoding it
  // every time.
  val PingTag = Message.MinTag
  val MinTag = PingTag+1
  val MaxTag = Message.MaxTag

  val NackFailure = Failure.rejected("The request was Nacked by the server")

  val Empty: Updatable[Try[Response]] = Updatable.empty()
}

/**
 * A ClientDispatcher for the mux protocol.
 */
private[twitter] class ClientDispatcher (
  name: String,
  trans: Transport[ChannelBuffer, ChannelBuffer],
  sr: StatsReceiver,
  failureDetectorConfig: FailureDetector.Config
) extends Service[Request, Response] {
  import ClientDispatcher._
  import Message.{MaxTag => _, MinTag => _, _}

  private[this] implicit val timer = DefaultTimer.twitter
  // Maintain the dispatcher's state, whose access is mediated
  // by the readLk and writeLk.
  @volatile private[this] var state: State = Dispatching
  private[this] val (readLk, writeLk) = {
    val lk = new ReentrantReadWriteLock
    (lk.readLock, lk.writeLock)
  }

  @volatile private[this] var canDispatch: Cap.State = Cap.Unknown

  private[this] val futureNackedException = Future.exception(NackFailure)

  // We pre-encode a ping message with the reserved ping tag
  // (PingTag) in order to avoid re-encoding this frequently sent
  // message. Since it uses ChannelBuffers, it maintains a read
  // cursor, and thus it is important that it is not used
  // concurrently. This happens to agree with the natural way you'd
  // use it, since a client can only have one outstanding ping per
  // tag.
  private[this] val pingMessage = {
    val buf = new ReadOnlyChannelBuffer(encode(Tping(PingTag)))
    buf.markReaderIndex()
    buf
  }
  private[this] val pingPromise = new AtomicReference[Promise[Unit]](null)

  private[this] val tags = TagSet(MinTag to MaxTag)
  private[this] val reqs = TagMap[Updatable[Try[Response]]](tags)
  private[this] val log = Logger.getLogger(getClass.getName)

  private[this] val gauge = sr.addGauge("current_lease_ms") {
    state match {
      case l: Leasing => l.remaining.inMilliseconds
      case _ => (Time.Top - Time.now).inMilliseconds
    }
  }
  private[this] val leaseCounter = sr.counter("leased")
  private[this] val drainingCounter = sr.counter("draining")
  private[this] val drainedCounter = sr.counter("drained")

  // We're extra paranoid about logging. The log handler is,
  // after all, outside of our control.
  private[this] def safeLog(msg: String, level: Level = Level.INFO): Unit =
    try {
      log.log(level, msg)
    } catch {
      case _: Throwable =>
    }

  private[this] def releaseTag(tag: Int): Option[Updatable[Try[Response]]] =
    reqs.unmap(tag) match {
      case None => None
      case some =>
        readLk.lock()
        if (state == Draining && tags.isEmpty) {
          drainedCounter.incr()
          safeLog(s"Finished draining a connection to $name", Level.FINE)
          readLk.unlock()

          writeLk.lock()
          state = Drained
          writeLk.unlock()
        } else {
          readLk.unlock()
        }

        if (some eq Empty) None else some
    }

  private[this] val receive: Message => Unit = {
    case RreqOk(tag, rep) =>
      for (p <- releaseTag(tag))
        p() = Return(Response(ChannelBufferBuf.Owned(rep)))
    case RreqError(tag, error) =>
      for (p <- releaseTag(tag))
        p() = Throw(ServerApplicationError(error))
    case RreqNack(tag) =>
      for (p <- releaseTag(tag))
        p() = Throw(NackFailure)

    case RdispatchOk(tag, _, rep) =>
      for (p <- releaseTag(tag))
        p() = Return(Response(ChannelBufferBuf.Owned(rep)))
    case RdispatchError(tag, _, error) =>
      for (p <- releaseTag(tag))
        p() = Throw(ServerApplicationError(error))
    case RdispatchNack(tag, _) =>
      for (p <- releaseTag(tag))
        p() = Throw(NackFailure)

    case Rerr(tag, error) =>
      for (p <- releaseTag(tag))
        p() = Throw(ServerError(error))

    case Rping(PingTag) =>
      val p = pingPromise.getAndSet(null)
      if (p != null)
        p.setDone()

    case Rping(tag) =>
      for (p <- releaseTag(tag))
        p() = Return(Response.empty)
    case Tping(tag) =>
      trans.write(encode(Rping(tag)))
    case Tdrain(tag) =>
      safeLog(s"Started draining a connection to $name", Level.FINE)
      drainingCounter.incr()
      // must be synchronized to avoid writing after Rdrain has been sent
      writeLk.lockInterruptibly()
      try {
        state = if (tags.nonEmpty) Draining else {
          safeLog(s"Finished draining a connection to $name", Level.FINE)
          drainedCounter.incr()
          Drained
        }
        trans.write(encode(Rdrain(tag)))
      } finally {
        writeLk.unlock()
      }

    case Tlease(Message.Tlease.MillisDuration, millis) =>
      writeLk.lock()

      try {
        state match {
          case Leasing(_) | Dispatching =>
            state = Leasing(Time.now + millis.milliseconds)
            log.fine(s"leased for ${millis.milliseconds} to ${trans.remoteAddress}")
            leaseCounter.incr()
         case Drained | Draining =>
           // Ignore the lease if we're in the process of draining, since
           // these are anyway irrecoverable states.
       }
     } finally {
       writeLk.unlock()
     }

    // Ignore lease types we don't understand. (They are advisory.)
    case Tlease(_, _) =>

    case m@Tmessage(tag) =>
      log.warning("Did not understand Tmessage[tag=%d] %s".format(tag, m))
      trans.write(encode(Rerr(tag, "badmessage")))
    case m@Rmessage(tag) =>
      val what = "Did not understand Rmessage[tag=%d] %s".format(tag, m)
      log.warning(what)
      for (p <- releaseTag(tag))
        p() = Throw(BadMessageException(what))
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
    val result = Throw(exc)
    for (tag <- tags) {
      /*
       * unmap the `tag` here to prevent the associated promise from
       * being fetched from the tag map again, and setting a value twice.
       */
      for (p <- reqs.unmap(tag)) p() = result
    }
  }

  def ping(): Future[Unit] = {
    val done = new Promise[Unit]
    if (pingPromise.compareAndSet(null, done)) {
      pingMessage.resetReaderIndex()
      // Note that we ignore any errors here. In practice this is fine
      // as (1) this will only happen when the session has anyway
      // died; (2) subsequent pings will use freshly allocated tags.
      trans.write(pingMessage) before done
    } else {
      val p = new Promise[Response]
      reqs.map(p) match {
        case None =>
          Future.exception(WriteException(new Exception("Exhausted tags")))
        case Some(tag) =>
          trans.write(encode(Tping(tag))) transform {
            case Return(()) =>
              p.unit
            case t@Throw(_) =>
              releaseTag(tag)
              Future.const(t)

          }
      }
    }
  }

  def apply(req: Request): Future[Response] = {
    readLk.lock()
    try state match {
      case Dispatching | Leasing(_) => dispatch(req)
      case Draining | Drained => futureNackedException
    } finally readLk.unlock()
  }

  /**
   * Dispatch a request.
   *
   * @param req the buffer representation of the request to be dispatched
   */
  private def dispatch(req: Request): Future[Response] = {
    val p = new Promise[Response]
    val couldDispatch = canDispatch

    val tag = reqs.map(p) match {
      case Some(t) => t
      case None =>
        return Future.exception(WriteException(new Exception("Exhausted tags")))
    }

    val msg =
      if (couldDispatch == Cap.No)
        Treq(tag, Some(Trace.id), BufChannelBuffer(req.body))
      else {
        val contexts = Contexts.broadcast.marshal() map { case (k, v) =>
          (BufChannelBuffer(k), BufChannelBuffer(v))
        }
        Tdispatch(tag, contexts.toSeq, req.destination, Dtab.local,
          BufChannelBuffer(req.body))
      }

    trans.write(encode(msg)) onFailure { case exc =>
      releaseTag(tag)
    } before {
      p.setInterruptHandler { case cause =>
        // We replace the current Updatable, if any, with a stand-in to reserve
        // the tag of discarded requests until Tdiscarded is acknowledged by the
        // peer.
        for (reqP <- reqs.maybeRemap(tag, Empty)) {
          trans.write(encode(Tdiscarded(tag, cause.toString)))
          reqP() = Throw(cause)
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

  private[this] val detector = {
    val close = () => trans.close(Time.now)
    val dsr = sr.scope("failuredetector")
    FailureDetector(failureDetectorConfig, ping, close, dsr)
  }

  override def status: Status =
    Status.worst(detector.status,
      trans.status match {
        case Status.Closed => Status.Closed
        case Status.Busy => Status.Busy
        case Status.Open =>
          readLk.lock()
          try state match {
            case Draining => Status.Busy
            case Drained => Status.Closed
            case leased@Leasing(_) if leased.expired => Status.Busy
            case Leasing(_) | Dispatching => Status.Open
          } finally readLk.unlock()
      }
    )

  override def close(deadline: Time): Future[Unit] = trans.close(deadline)
}
