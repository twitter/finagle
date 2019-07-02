package com.twitter.finagle.pool

import com.twitter.finagle._
import com.twitter.finagle.client.StackClient
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.util.{Future, Return, Throw, Time, Promise}
import java.util.concurrent.atomic.{AtomicReference, AtomicInteger}
import scala.annotation.tailrec

private[finagle] object SingletonPool {
  val role: Stack.Role = StackClient.Role.pool

  /**
   * Creates a [[com.twitter.finagle.Stackable]] [[com.twitter.finagle.pool.SingletonPool]].
   *
   * @param allowInterrupts forwards the parameter to the [[SingletonPool]] constructor.
   * See the constructor for semantics. Note, this isn't a [[Stack.Param]] since it's not
   * something we want to be configured by users, but rather it is a parameter for protocol
   * implementors.
   */
  def module[Req, Rep](allowInterrupts: Boolean): Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module1[param.Stats, ServiceFactory[Req, Rep]] {
      val role: Stack.Role = SingletonPool.role
      val description = "Maintain at most one connection"
      def make(_stats: param.Stats, next: ServiceFactory[Req, Rep]): ServiceFactory[Req, Rep] = {
        val param.Stats(sr) = _stats
        new SingletonPool(next, allowInterrupts, sr.scope("singletonpool"))
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
  private class RefcountedService[Req, Rep](underlying: Service[Req, Rep])
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
          Future.exception(Failure(new ServiceClosedException))
        case _ =>
          Future.Done
      }
  }

  private sealed trait State[-Req, +Rep]
  private case object Idle extends State[Any, Nothing]
  private case object Closed extends State[Any, Nothing]
  private case class Awaiting(done: Future[Unit]) extends State[Any, Nothing]
  private case class Open[Req, Rep](service: RefcountedService[Req, Rep]) extends State[Req, Rep]
}

/**
 * A pool that maintains at most one service from the underlying
 * ServiceFactory -- concurrent leases share the same, cached
 * service. A new Service is established whenever the service factory
 * fails or the current service has become unavailable.
 *
 * @param underlying the underlying shared resource which the pool manages.
 *
 * @param allowInterrupts Because the pool hands back a shared resource, it
 * may be useful to manage interrupts such that they are isolated from independent
 * service acquisition requests. Thus, setting this value to `false` will ensure that
 * an outstanding dispatch to the underlying resource from the pool is uninterruptible
 * but individual service acquisition requests are.
 *
 * @param statsReceiver the [[StatsReceiver]] that is used to report pool specific stats.
 */
class SingletonPool[Req, Rep](
  underlying: ServiceFactory[Req, Rep],
  allowInterrupts: Boolean,
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
  private[this] def connect(done: Promise[Unit], conn: ClientConnection): Unit = {
    def complete(newState: State[Req, Rep]) = state.get match {
      case s @ Awaiting(d) if d == done => state.compareAndSet(s, newState)
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
          Failure("Returned unavailable service", FailureFlags.Retryable)
            .withSource(Failure.Source.Role, SingletonPool.role)
        )

      case Return(svc) =>
        if (!complete(Open(new RefcountedService(svc))))
          svc.close()

        Future.Done
    })
  }

  // These two await* methods are required to trick the compiler into accepting
  // the definitions of 'apply' and 'close' as tail-recursive.
  private[this] def awaitApply(done: Future[Unit], conn: ClientConnection) = {
    // `f` represents a request to `underlying` to acquire a new service.
    val f = done.before(apply(conn))
    // if we allow interrupts, we return a direct handle to the process. Otherwise,
    // we returned a derivative future which can be interrupted immediately, but will
    // ensure the release of its interest in any ref-counted session that materializes.
    if (allowInterrupts) f
    else
      f.interruptible().onFailure {
        case _: Throwable =>
          // If this fails it is either because `f` failed or we were interrupted. If `f`
          // does succeed we need to release the ref-count since the caller will never
          // get a reference to the `Service` to release it.
          f.onSuccess(_.close())
      }
  }

  @tailrec
  final def apply(conn: ClientConnection): Future[Service[Req, Rep]] = state.get match {
    case Open(svc) if svc.status != Status.Closed =>
      // It is possible that the pool's state has changed by the time
      // we can return the service, so svc is possibly stale. We don't
      // attempt to resolve this race; rather, we let the lower layers deal
      // with it.
      svc.open()

    case s @ Open(svc) => // service died; try to reconnect.
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
      Future.exception(Failure(new ServiceClosedException))
  }

  /**
   * @inheritdoc
   *
   * The status of a [[SingletonPool]] is the worse of the
   * the underlying status and the status of the currently
   * cached service, if any.
   */
  override def status: Status =
    state.get match {
      case Closed => Status.Closed
      case Open(svc) =>
        // We don't account for closed services as these will
        // be reestablished on the next request.
        svc.status match {
          case Status.Closed => underlying.status
          case status => Status.worst(status, underlying.status)
        }
      case Idle | Awaiting(_) =>
        // This could also be Status.worst(underlying.status, Status.Busy(p));
        // in practice this probably won't make much of a difference, though,
        // since pending requests are anyway queued.
        underlying.status
    }

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
      case Idle =>
        if (!state.compareAndSet(Idle, Closed)) closeService(deadline)
        else Future.Done

      case s @ Open(svc) =>
        if (!state.compareAndSet(s, Closed)) closeService(deadline)
        else svc.close(deadline)

      case s @ Awaiting(done) =>
        if (!state.compareAndSet(s, Closed)) closeService(deadline)
        else {
          done.raise(new ServiceClosedException)
          Future.Done
        }

      case Closed =>
        Future.Done
    }
}
