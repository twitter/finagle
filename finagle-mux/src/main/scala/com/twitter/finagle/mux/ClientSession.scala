package com.twitter.finagle.mux

import com.twitter.conversions.time._
import com.twitter.finagle.mux.transport.Message
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.{Failure, Status}
import com.twitter.util.{Duration, Future, Promise, Time}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.logging.{Level, Logger}

/**
 * A ClientSession implements the state machine for a mux client session. It's
 * implemented as transport and, thus, can sit below a client dispatcher. It
 * transitions between various states based on the messages it processes
 * and can be in one of following states:
 *
 * `Dispatching`: The stable operating state of a Session. The `status` is
 * [[com.twitter.finagle.Status.Open]] and calls to `write` are passed on
 * to the transport below.
 *
 * `Draining`: When a session is `Draining` it has processed a `Tdrain`
 * message, but still has outstanding requests. In this state, we have
 * promised our peer not to send any more requests, thus the session's `status`
 * is [[com.twitter.finagle.Status.Busy]] and calls to `write` are nacked.
 *
 * `Drained`: When a session is fully drained; that is, it has received a
 * `Tdrain` and there are no more pending requests, the sessions's
 * state is set to `Drained`. In this state, the session is useless.
 * It is dead. Its `status` is set to [[com.twitter.finagle.Status.Closed]]
 * and calls to `write` are nacked.
 *
 * `Leasing`: When the session has processed a lease, its state is
 * set to `Leasing` which comprises the lease expiry time. This state
 * is equivalent to `Dispatching` except if the lease has expired.
 * At this time, the session's `status` is set to [[com.twitter.finagle.Status.Busy]].
 *
 * This can be composed below a `ClientDispatcher` to manages its session.
 *
 * @param trans The underlying transport.
 *
 * @param detectorConfig The config used to instantiate a failure detector over the
 * session. The detector is given control over the session's ping mechanism and its
 * status is reflected in the session's status.
 *
 * @param name The identifier for the session, used when logging.
 *
 * @param sr The [[com.twitter.finagle.StatsReceiver]] which the session uses to
 * export internal stats.
 */
private[twitter] class ClientSession(
    trans: Transport[Message, Message],
    detectorConfig: FailureDetector.Config,
    name: String,
    sr: StatsReceiver)
  extends Transport[Message, Message] {
  import ClientSession._

  // Maintain the sessions's state, whose access is mediated
  // by the readLk and writeLk.
  @volatile private[this] var state: State = Dispatching
  private[this] val (readLk, writeLk) = {
    val lk = new ReentrantReadWriteLock
    (lk.readLock, lk.writeLock)
  }

  // keeps track of outstanding Rmessages.
  private[this] val outstanding = new AtomicInteger()

  private[this] val pingMessage = new Message.PreEncodedTping
  private[this] val pingPromise = new AtomicReference[Promise[Unit]](null)

  private[this] val log = Logger.getLogger(getClass.getName)
  private[this] def safeLog(msg: String, level: Level = Level.INFO): Unit =
    try log.log(level, msg) catch {
      case _: Throwable =>
    }

  private[this] val leaseGauge = sr.addGauge("current_lease_ms") {
    state match {
      case l: Leasing => l.remaining.inMilliseconds
      case _ => (Time.Top - Time.now).inMilliseconds
    }
  }

  private[this] val leaseCounter = sr.counter("leased")
  private[this] val drainingCounter = sr.counter("draining")
  private[this] val drainedCounter = sr.counter("drained")

  /**
   * Processes mux control messages and transitions the state accordingly.
   * The transitions are synchronized with and reflected in `status`.
   */
  def processControlMsg(m: Message): Unit = m match {
    case Message.Tdrain(tag) =>
      if (log.isLoggable(Level.FINE))
        safeLog(s"Started draining a connection to $name", Level.FINE)
      drainingCounter.incr()

      writeLk.lockInterruptibly()
      try {
        state = if (outstanding.get() > 0) Draining else {
          if (log.isLoggable(Level.FINE))
            safeLog(s"Finished draining a connection to $name", Level.FINE)
          drainedCounter.incr()
          Drained
        }
        trans.write(Message.Rdrain(tag))
      } finally writeLk.unlock()

    case Message.Tlease(Message.Tlease.MillisDuration, millis) =>
      writeLk.lock()
      try state match {
        case Leasing(_) | Dispatching =>
          state = Leasing(Time.now + millis.milliseconds)
          if (log.isLoggable(Level.FINE))
            safeLog(s"leased for ${millis.milliseconds} to $name", Level.FINE)
          leaseCounter.incr()
        case Draining | Drained =>
          // Ignore the lease if we're closed, since these are anyway
          // a irrecoverable states.
      } finally writeLk.unlock()

    case Message.Tping(tag) => trans.write(Message.Rping(tag))

    case _ => // do nothing
  }

  private[this] def processRmsg(msg: Message): Unit = msg match {
    case Message.Rping(Message.PingTag) =>
      val p = pingPromise.getAndSet(null)
      if (p != null) p.setDone()

    case Message.Rerr(Message.PingTag, err) =>
      val p = pingPromise.getAndSet(null)
      if (p != null) p.setException(ServerError(err))

    // Move the session to `Drained`, effectively closing the session,
    // if we were `Draining` our session.
    case Message.Rmessage(_) =>
      readLk.lock()
      if (outstanding.decrementAndGet() == 0 && state == Draining) {
        readLk.unlock()
        writeLk.lock()
        try {
          drainedCounter.incr()
          if (log.isLoggable(Level.FINE))
            safeLog(s"Finished draining a connection to $name", Level.FINE)
          state = Drained
        } finally writeLk.unlock()
      } else {
        readLk.unlock()
      }

    case _ => // do nothing.
  }

  private[this] val processTwriteFail: Throwable => Unit = { _ =>
    outstanding.decrementAndGet()
  }

  private[this] def processAndWrite(msg: Message): Future[Unit] = msg match {
    case _: Message.Treq | _: Message.Tdispatch =>
      outstanding.incrementAndGet()
      trans.write(msg).onFailure(processTwriteFail)
    case _ => trans.write(msg)
  }

  private[this] def processRead(msg: Message) = msg match {
    case m@Message.Rmessage(_) => processRmsg(m)
    case m@Message.ControlMessage(_) => processControlMsg(m)
    case _ => // do nothing.
  }

  /**
   * Write to the underlying transport if our state permits,
   * otherwise return a nack.
   */
  def write(msg: Message): Future[Unit] = {
    readLk.lock()
    try state match {
      case Dispatching | Leasing(_) => processAndWrite(msg)
      case Draining | Drained => FutureNackException
    } finally readLk.unlock()
  }

  def read(): Future[Message] = trans.read().onSuccess(processRead)

  /**
   * Send a mux Tping to our peer. Note, only one outstanding ping is
   * permitted, subsequent calls to ping are failed fast.
   */
  def ping(): Future[Unit] = {
    val done = new Promise[Unit]
    if (pingPromise.compareAndSet(null, done)) {
      trans.write(pingMessage).before(done)
    } else {
      FuturePingNack
    }
  }

  private[this] val detector = FailureDetector(
    detectorConfig, ping, sr.scope("failuredetector"))

  def status: Status = Status.worst(detector.status, {
    readLk.lock()
    try state match {
      case Draining => Status.Busy
      case Drained => Status.Closed
      case leased@Leasing(_) if leased.expired => Status.Busy
      case Leasing(_) | Dispatching => Status.Open
    } finally readLk.unlock()
  })

  val onClose = trans.onClose
  def localAddress = trans.localAddress
  def remoteAddress = trans.remoteAddress
  def peerCertificate = trans.peerCertificate

  def close(deadline: Time): Future[Unit] = {
    leaseGauge.remove()
    trans.close(deadline)
  }
}

private object ClientSession {
  val FutureNackException = Future.exception(
    Failure.rejected("The request was Nacked by the server"))

  val FuturePingNack = Future.exception(Failure(
    "A ping is already oustanding on this session."))

  sealed trait State
  case object Dispatching extends State
  case object Draining extends State
  case object Drained extends State
  case class Leasing(end: Time) extends State {
    def remaining: Duration = end.sinceNow
    def expired: Boolean = end < Time.now
  }
}