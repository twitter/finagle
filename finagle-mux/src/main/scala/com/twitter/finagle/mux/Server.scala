package com.twitter.finagle.mux

import com.twitter.app.GlobalFlag
import com.twitter.finagle._
import com.twitter.finagle.context.{Contexts, RemoteInfo}
import com.twitter.finagle.mux.lease.exp.{Lessee, Lessor, nackOnExpiredLease}
import com.twitter.finagle.mux.transport.{Message, MuxFailure}
import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.tracing.{NullTracer, Trace, Tracer}
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.util.DefaultTimer
import com.twitter.logging.HasLogLevel
import com.twitter.util._
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import java.util.concurrent.ConcurrentHashMap
import java.util.logging.{Level, Logger}
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.util.control.NoStackTrace

/**
 * Indicates that a client requested that a given request be discarded.
 *
 * This implies that the client issued a Tdiscarded message for a given tagged
 * request, as per [[com.twitter.finagle.mux]].
 */
case class ClientDiscardedRequestException(why: String)
  extends Exception(why)
  with HasLogLevel
  with NoStackTrace {
  def logLevel: com.twitter.logging.Level = com.twitter.logging.Level.DEBUG
}

object gracefulShutdownEnabled
    extends GlobalFlag(
      true,
      "Graceful shutdown enabled. " +
        "Temporary measure to allow servers to deploy without hurting clients."
    )

/**
 * A tracker is responsible for tracking pending transactions
 * and coordinating draining.
 */
private class Tracker[T] {
  private[this] val pending = new ConcurrentHashMap[Int, Future[Unit]]
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
    else if (!state.compareAndSet(n, n + 1)) enter()
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
    } else if (!state.compareAndSet(n, n - 1)) exit()
  }

  /**
   * Track a transaction. `track` manages the lifetime of a tag
   * including its reply and write. Function `process` handles the result
   * of `reply`. The tag is freed once a client receives the reply, and, since
   * write completion is not synchronous with processing the next
   * request, there is a race between acknowledging the write and
   * receiving the next request from the client (which may then reuse
   * the tag); We also can't complete draining until we've acknowledged
   * the write for the last request processed.
   *
   * @note `track` isn't synchronized across threads so this may have
   * races in a multithreaded environment. In our case, each instance
   * is owned by exactly one thread (i.e. we inherit netty's threading
   * model).
   */
  def track(tag: Int, reply: Future[T])(process: Try[T] => Future[Unit]): Future[Unit] = {
    if (!enter()) return reply.transform(process)

    val f = reply.transform(process)
    pending.put(tag, f)
    f.ensure {
      pending.remove(tag)
      exit()
    }
  }

  /**
   * Retrieve the value for the pending transaction matching `tag`.
   */
  def get(tag: Int): Option[Future[Unit]] =
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
    math.abs(state.get) - 1
}

private[finagle] object ServerDispatcher {

  /**
   * Construct a new request-response dispatcher.
   */
  def newRequestResponse(
    trans: Transport[Message, Message],
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
    trans: Transport[Message, Message],
    service: Service[Request, Response]
  ): ServerDispatcher =
    newRequestResponse(trans, service, Lessor.nil, NullTracer, NullStatsReceiver)

  object State extends Enumeration {
    val Open, Draining, Closed = Value
  }
}

/**
 * A dispatcher for the Mux protocol. In addition to multiplexing, the dispatcher
 * handles concerns of leasing and draining.
 */
private[finagle] class ServerDispatcher(
  trans: Transport[Message, Message],
  service: Service[Message, Message],
  lessor: Lessor, // the lessor that the dispatcher should register with in order to get leases
  tracer: Tracer,
  statsReceiver: StatsReceiver
) extends Closable
    with Lessee {
  import ServerDispatcher.State

  private[this] implicit val injectTimer = DefaultTimer
  private[this] val tracker = new Tracker[Message]
  private[this] val log = Logger.getLogger(getClass.getName)
  private[this] val duplicateTagCounter = statsReceiver.counter("duplicate_tag")
  private[this] val orphanedTdiscardCounter = statsReceiver.counter("orphaned_tdiscard")
  private[this] val legacyReqCounter = statsReceiver.counter("legacy_req")

  private[this] val state: AtomicReference[State.Value] =
    new AtomicReference(State.Open)

  @volatile private[this] var leaseExpiration: Time = Time.Top
  lessor.register(this)

  private[this] def write(m: Message): Future[Unit] =
    trans.write(m)

  private[this] def nackLeaseExpiration: Boolean =
    nackOnExpiredLease() && leaseExpiration <= Time.now

  private[this] def shouldNack: Boolean =
    tracker.isDraining || nackLeaseExpiration

  private[this] def process(m: Message): Unit = {
    if (m.isInstanceOf[Message.Treq])
      legacyReqCounter.incr()

    m match {
      case (_: Message.Tdispatch | _: Message.Treq) if !shouldNack =>
        lessor.observeArrival()
        val elapsed = Stopwatch.start()

        val reply: Try[Message] => Future[Unit] = {
          case Return(rep) =>
            lessor.observe(elapsed())
            write(rep)
          case Throw(exc) =>
            log.log(Level.WARNING, s"Error processing message $m", exc)
            write(Message.Rerr(m.tag, exc.toString))
        }

        if (!tracker.isTracking(m.tag)) {
          tracker.track(m.tag, service(m))(reply)

        } else {
          // This can mean two things:
          //
          // 1. We have a pathalogical client which is sending duplicate tags.
          // We push the responsibility of resolving the duplicate on the client
          // and service the request.
          //
          // 2. We lost a race with the client where it reused a tag before we were
          // able to cleanup the tracker. This is possible since we cleanup state on
          // write closures which can be executed on a separate thread from the event
          // loop thread (in netty3). We take extra precaution in the `ChannelTransport.write`
          // to a avoid this, but technically it isn't guaranteed by netty3.
          //
          // In both cases, we forfeit the ability to track (and thus drain or interrupt)
          // the request, but we can still service it.
          log.fine(s"Received duplicate tag ${m.tag} from client ${trans.remoteAddress}")
          duplicateTagCounter.incr()
          service(m).transform(reply)
        }

      // Dispatch when shouldNack
      case d: Message.Tdispatch =>
        write(Message.RdispatchNack(d.tag, Nil))
      case r: Message.Treq =>
        write(Message.RreqNack(r.tag))

      case _: Message.Tping =>
        service(m).respond {
          case Return(rep) => write(rep)
          case Throw(exc) => write(Message.Rerr(m.tag, exc.toString))
        }

      case Message.Tdiscarded(tag, why) =>
        tracker.get(tag) match {
          case Some(reply) =>
            reply.raise(new ClientDiscardedRequestException(why))
          case None =>
            orphanedTdiscardCounter.incr()
        }

      case Message.Rdrain(1) if state.get == State.Draining =>
        tracker.drain()

      case m: Message =>
        val rerr = Message.Rerr(m.tag, s"Unexpected mux message type ${m.typ}")
        write(rerr)
    }
  }

  private[this] def loop(): Unit =
    Future.each(trans.read()) { msg =>
      val save = Local.save()
      process(msg)
      Local.restore(save)
    } ensure { hangup(Time.now) }

  Local.letClear {
    Trace.letTracer(tracer) {
      Contexts.local.let(RemoteInfo.Upstream.AddressCtx, trans.remoteAddress) {
        trans.peerCertificate match {
          case None => loop()
          case Some(cert) =>
            Contexts.local.let(Transport.peerCertCtx, cert) {
              loop()
            }
        }
      }
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
    case s @ (State.Draining | State.Open) =>
      if (!state.compareAndSet(s, State.Closed)) hangup(deadline)
      else {
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
    val done = write(Message.Tdrain(1)) before
      tracker.drained.by(deadline) before
      trans.close(deadline)
    done.transform {
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
   * Emit a lease to the clients of this server. If the `nackOnExpiredLease` flag
   * is set to `true` the server will nack all requests once the lease expires and
   * continue to do so until a new non-zero lease is issued.
   */
  def issue(howlong: Duration): Unit = {
    require(howlong >= Message.Tlease.MinLease)

    synchronized {
      leaseExpiration = Time.now + howlong
      // This needs to be written in the synchronized block to avoid a race where
      // two racing calls to `issue` cause the client and servers understanding of
      // leaseExpiration to become inconsistent do to racing writes
      write(Message.Tlease(howlong.min(Message.Tlease.MaxLease)))
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
private[finagle] object Processor extends Filter[Message, Message, Request, Response] {
  import Message._

  private[this] def dispatch(
    tdispatch: Message.Tdispatch,
    service: Service[Request, Response]
  ): Future[Message] = {

    Contexts.broadcast.letUnmarshal(tdispatch.contexts) {
      if (tdispatch.dtab.nonEmpty)
        Dtab.local ++= tdispatch.dtab
      service(Request(tdispatch.dst, tdispatch.contexts, tdispatch.req)).transform {
        case Return(rep) =>
          Future.value(RdispatchOk(tdispatch.tag, rep.contexts, rep.body))

        // Previously, all Restartable failures were sent as RdispatchNack
        // messages. In order to keep backwards compatibility with clients that
        // do not look for MuxFailures, this behavior is left alone. additional
        // MuxFailure flags are still sent.
        case Throw(f: Failure) if f.isFlagged(Failure.Restartable) =>
          val mFail = MuxFailure.fromThrow(f)
          Future.value(RdispatchNack(tdispatch.tag, mFail.contexts))

        case Throw(exc) =>
          val mFail = MuxFailure.fromThrow(exc)
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

        case Throw(f: Failure) if f.isFlagged(Failure.Restartable) =>
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
