package com.twitter.finagle.pool

import com.twitter.finagle._
import com.twitter.finagle.service.FailedService
import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.util.{Future, Return, Throw, Time, Promise}
import java.util.concurrent.atomic.{AtomicReference, AtomicInteger}
import scala.annotation.tailrec
import scala.collection.immutable

private[finagle] object SingletonPool {
  val role = Stack.Role("SingletonPool")

  /**
   * Creates a [[com.twitter.finagle.Stackable]] [[com.twitter.finagle.pool.SingletonPool]].
   */
  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module1[param.Stats, ServiceFactory[Req, Rep]] {
      val role = SingletonPool.role
      val description = "Maintain at most one connection"
      def make(_stats: param.Stats, next: ServiceFactory[Req, Rep]) = {
        val param.Stats(sr) = _stats
        new SingletonPool(next, sr.scope("singletonpool"))
      }
    }

  /**
   * A wrapper service to maintain a reference count. The count is
   * set to 1 on construction; the count is decreased for each
   * 'close'. Additional references are attained by calling 'open'.
   * When the count reaches 0, the underlying service is closed.
   *
   * @note This implementation doesn't prevent the reference count
   * from crossing the 0 boundary multiple times -- it may thus call
   * 'close' on the underlying service multiple times.
   */
  class RefcountedService[Req, Rep](underlying: Service[Req, Rep])
      extends ServiceProxy[Req, Rep](underlying) {
    private[this] val count = new AtomicInteger(1)
    private[this] val future = Future.value(this)

    def open(): Future[Service[Req, Rep]] = {
      count.incrementAndGet()
      future
    }

    override def close(deadline: Time): Future[Unit] =
      count.decrementAndGet() match {
        case 0 => underlying.close(deadline)
        case n if n < 0 =>
          // This is technically an API usage error.
          count.incrementAndGet()
          Future.exception(Failure.Cause(new ServiceClosedException))
        case _ =>
          Future.Done
      }
  }

  sealed trait State[-Req, +Rep]
  case object Idle extends State[Any, Nothing]
  case object Closed extends State[Any, Nothing]
  case class Awaiting(done: Future[Unit]) extends State[Any, Nothing]
  case class Open[Req, Rep](service: RefcountedService[Req, Rep]) extends State[Req, Rep]
}

/**
 * A pool that maintains at most one service from the underlying
 * ServiceFactory -- concurrent leases share the same, cached
 * service. A new Service is established whenever the service factory
 * fails or the current service has become unavailable.
 */
class SingletonPool[Req, Rep](
  underlying: ServiceFactory[Req, Rep],
  statsReceiver: StatsReceiver)
extends ServiceFactory[Req, Rep] {
  import SingletonPool._

  private[this] val scoped = statsReceiver.scope("connects")
  private[this] val failStat = scoped.counter("fail")
  private[this] val deadStat = scoped.counter("dead")

  private[this] val state = new AtomicReference(Idle: State[Req, Rep])

  /**
   * Attempt to connect with the underlying factory and CAS the
   * state Awaiting(done) => Open()|Idle depending on the outcome.
   * Connect satisfies passed-in promise when the process is
   * complete.
   */
  private[this] def connect(done: Promise[Unit], conn: ClientConnection) {
    def complete(newState: State[Req, Rep]) = state.get match {
      case s@Awaiting(d) if d == done => state.compareAndSet(s, newState)
      case Idle | Closed | Awaiting(_) | Open(_) => false
    }

    done.become(underlying(conn) transform {
      case Throw(exc) =>
        failStat.incr()
        complete(Idle)
        Future.exception(exc)

      case Return(svc) if svc.status == Status.Closed =>
        // If we are returned a closed service, we treat the connect
        // as a failure. This is both correct -- the connect did fail -- and
        // also prevents us from entering potentially infinite loops where
        // every returned service is unavailable, which then causes the
        // follow-on apply() to attempt to reconnect.
        deadStat.incr()
        complete(Idle)
        svc.close()
        Future.exception(
          Failure.Cause("Returned unavailable service")
            .withSource("Role", SingletonPool.role))

      case Return(svc) =>
        if (!complete(Open(new RefcountedService(svc))))
          svc.close()

        Future.Done
    })
  }

  // These two await* methods are required to trick the compiler into accepting
  // the definitions of 'apply' and 'close' as tail-recursive.
  private[this] def awaitApply(done: Future[Unit], conn: ClientConnection) =
    done before apply(conn)

  @tailrec
  final def apply(conn: ClientConnection): Future[Service[Req, Rep]] = state.get match {
    case Open(svc) if svc.status != Status.Closed =>
      // It is possible that the pool's state has changed by the time
      // we can return the service, so svc is possibly stale. We don't
      // attempt to resolve this race; rather, we let the lower layers deal
      // with it.
      svc.open()

    case s@Open(svc) => // service died; try to reconnect.
      if (state.compareAndSet(s, Idle))
        svc.close()
      apply(conn)

    case Idle =>
      val done = new Promise[Unit]
      if (state.compareAndSet(Idle, Awaiting(done))) {
        connect(done, conn)
        awaitApply(done, conn)
      } else {
        apply(conn)
      }

    case Awaiting(done) =>
      awaitApply(done, conn)

    case Closed =>
      Future.exception(Failure.Cause(new ServiceClosedException))
  }

  /**
   * @inheritdoc
   *
   * A SingletonPool is available when it is not closed and the underlying
   * factory is also available.
   */
  override def status: Status = 
    if (state.get != Closed) underlying.status
    else Status.Closed

  /**
   * @inheritdoc
   *
   * SingletonPool closes asynchronously; the underlying connection is
   * closed once all references are returned.
   */
  final def close(deadline: Time): Future[Unit] =
    closeService(deadline) before underlying.close(deadline)

  @tailrec
  private[this] def closeService(deadline: Time): Future[Unit] = 
    state.get match {
      case s@Idle =>
        if (!state.compareAndSet(s, Closed)) closeService(deadline)
        else Future.Done
  
      case s@Open(svc) =>
        if (!state.compareAndSet(s, Closed)) closeService(deadline)
        else svc.close(deadline)
  
      case s@Awaiting(done) =>
        if (!state.compareAndSet(s, Closed)) closeService(deadline) else {
          done.raise(new ServiceClosedException)
          Future.Done
        }
  
      case Closed =>
        Future.Done
    }
}
