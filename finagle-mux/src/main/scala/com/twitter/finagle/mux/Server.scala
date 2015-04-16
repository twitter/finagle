package com.twitter.finagle.mux

import com.twitter.app.GlobalFlag
import com.twitter.conversions.time._
import com.twitter.finagle._
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.mux.lease.exp.{Lessor, Lessee, nackOnExpiredLease}
import com.twitter.finagle.netty3.{BufChannelBuffer, ChannelBufferBuf}
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver}
import com.twitter.finagle.tracing.{NullTracer, Trace, Tracer}
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.util.{DefaultLogger, DefaultTimer}
import com.twitter.util._
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicReference, AtomicInteger}
import java.util.logging.{Level, Logger}
import org.jboss.netty.buffer.ChannelBuffer
import scala.annotation.tailrec
import scala.collection.JavaConverters._

/**
 * Indicates that a client requested that a given request be discarded.
 *
 * This implies that the client issued a Tdiscarded message for a given tagged
 * request, as per [[com.twitter.finagle.mux]].
 */
case class ClientDiscardedRequestException(why: String) extends Exception(why)

object gracefulShutdownEnabled extends GlobalFlag(true, "Graceful shutdown enabled. " +
  "Temporary measure to allow servers to deploy without hurting clients.")

/**
 * A tracker is responsible for tracking pending transactions
 * and coordinating draining.
 */
private class Tracker[T] {
  private[this] val pending = new ConcurrentHashMap[Int, Future[T]]
  private[this] val _drained: Promise[Unit] = new Promise

  // The state of a tracker is a single integer. Its absolute
  // value minus one indicates the number of pending requests.
  // A negative value indicates that the tracker is draining.
  // Negative values cannot transition to positive values.
  private[this] val state = new AtomicInteger(1)

  /**
   * Try to enter a transaction, returning false if the
   * tracker is draining.
   */
  @tailrec
  private[this] def enter(): Boolean = {
    val n = state.get
    if (n <= 0) false
    else if (!state.compareAndSet(n, n+1)) enter()
    else true
  }

  /**
   * Exit an entered transaction.
   */
  @tailrec
  private[this] def exit(): Unit = {
    val n = state.get
    if (n < 0) {
      if (state.incrementAndGet() == -1)
        _drained.setDone()
    } else if (!state.compareAndSet(n, n-1)) exit()
  }

  private[this] val closedExit = (_: Try[Unit]) => exit()

  /**
   * Track a transaction. `track` manages the lifetime of a tag
   * and its reply. Function `process` handles the result of `reply`.
   * The ordering here is important: the tag is relinquished after
   * `reply` is satisfied but before `process` is invoked, but is still
   * considered pending until `process` completes. This is because:
   * (1) the tag is freed once a client receives the reply, and, since
   * write completion is not synchronous with processing the next
   * request, there is a race between acknowledging the write and
   * receiving the next request from the client (which may then reuse
   * the tag); (2) we can't complete draining until we've acknowledged
   * the write for the last request processed.
   */
  def track(tag: Int, reply: Future[T])(process: Try[T] => Future[Unit]): Future[Unit] = {
    if (!enter()) return reply.transform(process)

    pending.put(tag, reply)
    reply transform { r =>
      pending.remove(tag)
      process(r).respond(closedExit)
    }
  }

  /**
   * Retrieve the value for the pending request matching `tag`.
   */
  def get(tag: Int): Option[Future[T]] =
    Option(pending.get(tag))

  /**
   * Returns the set of current tags.
   */
  def tags: Set[Int] =
    pending.keySet.asScala.toSet

  /**
   * Initiate the draining protocol. After `drain` is called, future
   * requests for tracking are dropped. [[drained]] is satisified
   * when the number of pending requests reaches 0.
   */
  @tailrec
  final def drain(): Unit = {
    val n = state.get
    if (n < 0) return

    if (!state.compareAndSet(n, -n)) drain()
    else if (n == 1) _drained.setDone()
  }

  /**
   * True when the tracker is in draining state.
   */
  def isDraining: Boolean = state.get < 0

  /**
   * Satisifed when the tracker has completed the draining protocol,
   * as described in [[drain]].
   */
  def drained: Future[Unit] = _drained

  /**
   * Tests whether the given tag is actively tracked.
   */
  def isTracking(tag: Int): Boolean = pending.containsKey(tag)

  /**
   * The number of tracked tags.
   */
  def npending: Int =
    math.abs(state.get)-1
}

private[twitter] object ServerDispatcher {
  /**
   * Construct a new request-response dispatcher.
   */
  def newRequestResponse(
    trans: Transport[ChannelBuffer, ChannelBuffer],
    service: Service[Request, Response],
    lessor: Lessor,
    tracer: Tracer,
    statsReceiver: StatsReceiver
  ): ServerDispatcher =
    new ServerDispatcher(trans, Processor andThen service, lessor, tracer, statsReceiver)

  /**
   * Construct a new request-response dispatcher with a
   * null lessor, tracer, and statsReceiver.
   */
  def newRequestResponse(
    trans: Transport[ChannelBuffer, ChannelBuffer],
    service: Service[Request, Response]
  ): ServerDispatcher =
    newRequestResponse(trans, service, Lessor.nil, NullTracer, NullStatsReceiver)

  val Epsilon = 1.second

  object State extends Enumeration {
    val Open, Draining, Closed = Value
  }
}

/**
 * A dispatcher for the Mux protocol. In addition to multiplexing, the dispatcher
 * handles concerns of leasing and draining.
 */
private[twitter] class ServerDispatcher(
    trans: Transport[ChannelBuffer, ChannelBuffer],
    service: Service[Message, Message],
    lessor: Lessor, // the lessor that the dispatcher should register with in order to get leases
    tracer: Tracer,
    statsReceiver: StatsReceiver
) extends Closable with Lessee {
  import Message._
  import ServerDispatcher.State

  private[this] implicit val injectTimer = DefaultTimer.twitter
  private[this] val tracker = new Tracker[Message]
  private[this] val log = Logger.getLogger(getClass.getName)

  private[this] val state: AtomicReference[State.Value] =
    new AtomicReference(State.Open)

  @volatile private[this] var lease = Tlease.MaxLease
  @volatile private[this] var curElapsed = NilStopwatch.start()
  lessor.register(this)

  private[this] def write(m: Message): Future[Unit] =
    trans.write(encode(m))

  private[this] def isAccepting: Boolean =
    !tracker.isDraining && (!nackOnExpiredLease() || (lease > Duration.Zero))

  private[this] def process(m: Message): Unit = m match {
    case (_: Tdispatch | _: Treq) if isAccepting =>
      // A misbehaving client is sending duplicate pending tags.
      // Note that, since the client is managing multiple outstanding
      // requests for this tag, and we're returning an Rerr here, there
      // are no guarantees about client behavior in this case. Possibly
      // we should terminate the session in this case.
      //
      // TODO: introduce uniform handling of tag tracking
      // (across all request types), and also uniform handling
      // (e.g., session termination).
      if (tracker.isTracking(m.tag)) {
        log.warning(s"Received duplicate tag ${m.tag} from client ${trans.remoteAddress}")
        write(Rerr(m.tag, s"Duplicate tag ${m.tag}"))
        return
      }

      lessor.observeArrival()
      val elapsed = Stopwatch.start()
      tracker.track(m.tag, service(m)) {
        case Return(rep) =>
          lessor.observe(elapsed())
          write(rep)
        case Throw(exc) =>
          log.log(Level.WARNING, s"Error processing message $m", exc)
          write(Rerr(m.tag, exc.toString))
      }

    // Dispatch when !isAccepting
    case d: Tdispatch =>
      write(RdispatchNack(d.tag, Nil))
    case r: Treq =>
      write(RreqNack(r.tag))

    case _: Tping =>
      service(m) respond {
        case Return(rep) => write(rep)
        case Throw(exc) => write(Rerr(m.tag, exc.toString))
      }

    case Tdiscarded(tag, why) =>
      tracker.get(tag) match {
        case Some(reply) =>
          reply.raise(new ClientDiscardedRequestException(why))
        case None =>
      }

    case Rdrain(1) if state.get == State.Draining =>
      tracker.drain()

    case m@Tmessage(tag) =>
      val msg = Rerr(tag, f"Did not understand Tmessage ${m.typ}%d")
      write(msg)
  }

  Local.letClear {
    Trace.letTracer(tracer) {
      Future.each(trans.read) { buf =>
        val save = Local.save()
        process(decode(buf))
        Local.restore(save)
      } ensure { hangup(Time.now) }
    }
  }

  trans.onClose respond { res =>
    val exc = res match {
      case Return(exc) => exc
      case Throw(exc) => exc
    }
    val cancelledExc = new CancelledRequestException(exc)
    for (tag <- tracker.tags; f <- tracker.get(tag))
      f.raise(cancelledExc)

    service.close()
    lessor.unregister(this)

    state.get match {
      case State.Open =>
        statsReceiver.counter("clienthangup").incr()
      case (State.Draining | State.Closed) =>
        statsReceiver.counter("serverhangup").incr()
    }
  }

  @tailrec
  private[this] def hangup(deadline: Time): Future[Unit] = state.get match {
    case State.Closed => Future.Done
    case s@(State.Draining | State.Open) =>
      if (!state.compareAndSet(s, State.Closed)) hangup(deadline) else {
        trans.close(deadline)
      }
    }

  def close(deadline: Time): Future[Unit] = {
    if (!state.compareAndSet(State.Open, State.Draining))
      return trans.onClose.unit

    if (!gracefulShutdownEnabled()) {
      // In theory, we can do slightly better here.
      // (i.e., at least try to wait for requests to drain)
      // but instead we should just disable this flag.
      return hangup(deadline)
    }

    statsReceiver.counter("draining").incr()
    val done = write(Tdrain(1)) before
      tracker.drained.within(deadline-Time.now) before
      trans.close(deadline)
    done transform {
      case Return(_) =>
        statsReceiver.counter("drained").incr()
        Future.Done
      case Throw(_: ChannelClosedException) =>
        Future.Done
      case Throw(_) =>
        hangup(deadline)
    }
  }

  /**
    * Emit a lease to the clients of this server.  If howlong is less than or
    * equal to 0, also nack all requests until a new lease is issued.
    */
  def issue(howlong: Duration): Unit = {
    require(howlong >= Tlease.MinLease)

    synchronized {
      val diff = (lease - curElapsed()).abs
      if (diff > ServerDispatcher.Epsilon) {
        curElapsed = Stopwatch.start()
        lease = howlong
        write(Tlease(howlong min Tlease.MaxLease))
      } else if ((howlong < Duration.Zero) && (lease > Duration.Zero)) {
        curElapsed = Stopwatch.start()
        lease = howlong
      }
    }
  }

  def npending: Int = tracker.npending
}

/**
 * Processor handles request, dispatch, and ping messages. Request
 * and dispatch messages are passed onto the request-response in the
 * filter chain. Pings are answered immediately in the affirmative.
 *
 * (This arrangement permits interpositioning other filters to modify ping
 * or dispatch behavior, e.g., for testing.)
 */
private object Processor extends Filter[Message, Message, Request, Response] {
  import Message._

  private[this] def dispatch(tdispatch: Tdispatch, service: Service[Request, Response]): Future[Message] = {
    val Tdispatch(tag, contexts, dst, dtab, bytes) = tdispatch

    val contextBufs = contexts map { case (k, v) =>
      ChannelBufferBuf(k) -> ChannelBufferBuf(v)
    }
    Contexts.broadcast.letUnmarshal(contextBufs) {
      if (dtab.length > 0)
        Dtab.local ++= dtab
      service(Request(dst, ChannelBufferBuf.Owned(bytes))) transform {
        case Return(rep) =>
          Future.value(RdispatchOk(tag, Nil, BufChannelBuffer(rep.body)))

        case Throw(f: Failure) if f.isFlagged(Failure.Restartable) =>
          Future.value(RdispatchNack(tag, Nil))

        case Throw(exc) =>
          Future.value(RdispatchError(tag, Nil, exc.toString))
      }
    }
  }

  private[this] def dispatch(treq: Treq, service: Service[Request, Response]): Future[Message] = {
    val Treq(tag, traceId, bytes) = treq
    Trace.letIdOption(traceId) {
      service(Request(Path.empty, ChannelBufferBuf.Owned(bytes))) transform {
        case Return(rep) =>
          Future.value(RreqOk(tag, BufChannelBuffer(rep.body)))

        case Throw(f: Failure) if f.isFlagged(Failure.Restartable) =>
          Future.value(RreqNack(tag))

        case Throw(exc) =>
          Future.value(RreqError(tag, exc.toString))
      }
    }
  }

  def apply(req: Message, service: Service[Request, Response]): Future[Message] = req match {
    case d: Tdispatch => dispatch(d, service)
    case r: Treq => dispatch(r, service)
    case Tping(tag) => Future.value(Rping(tag))
    case m => Future.exception(new IllegalArgumentException(s"Cannot process message $m"))
  }
}
