package com.twitter.finagle.service

import com.twitter.finagle._
import com.twitter.finagle.stats.{Counter, StatsReceiver}
import com.twitter.finagle.util.AsyncLatch
import com.twitter.util.{Duration, Promise, Future, NullTimerTask, Timer, Time}
import java.util.concurrent.atomic.AtomicBoolean

object ExpiringService {
  val role = Stack.Role("Expiration")

  /**
   * A class eligible for configuring a [[com.twitter.finagle.Stackable]]
   * [[com.twitter.finagle.service.ExpiringService]].
   *
   * @param idleTime max duration for which a connection is allowed to be idle.
   * @param lifeTime max lifetime of a connection.
   */
  case class Param(idleTime: Duration, lifeTime: Duration) {
    def mk(): (Param, Stack.Param[Param]) =
      (this, Param.param)
  }
  object Param {
    implicit val param = Stack.Param(Param(Duration.Top, Duration.Top))
  }

  private[finagle] def server[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module3[Param, param.Timer, param.Stats, ServiceFactory[Req, Rep]] {
      val role = ExpiringService.role
      val description = "Expire a service after a certain amount of idle time"
      def make(
        _param: Param,
        _timer: param.Timer,
        _stats: param.Stats,
        next: ServiceFactory[Req, Rep]
      ) = {
        val param.Timer(timer) = _timer
        val ExpiringService.Param(idleTime, lifeTime) = _param
        val param.Stats(statsReceiver) = _stats

        val idle = if (idleTime.isFinite) Some(idleTime) else None
        val life = if (lifeTime.isFinite) Some(lifeTime) else None

        (idle, life) match {
          case (None, None) => next
          case _ =>
            new ServiceFactoryProxy(next) {
              override def apply(conn: ClientConnection): Future[Service[Req, Rep]] =
                self(conn).map { service =>
                  new ExpiringService(service, idle, life, timer, statsReceiver) {
                    def onExpire(): Unit = {
                      conn.close()
                    }
                  }
                }
            }
        }
      }
    }

  // scoped this way for testing
  private[service] def closeOnReleaseSvc[Req, Rep](
    service: Service[Req, Rep]
  ): Service[Req, Rep] = {
    new ClosableService(service) {
      def closedException = WriteException(new ServiceClosedException)
    }
  }

  /**
   * Creates a [[com.twitter.finagle.Stackable]] [[com.twitter.finagle.service.ExpiringService]]
   * which simply extracts the service from the underlying `ServiceFactory` and calls
   * wraps it in an ExpiringService.
   */
  private[finagle] def client[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module3[Param, param.Timer, param.Stats, ServiceFactory[Req, Rep]] {
      val role = ExpiringService.role
      val description = "Expire a service after a certain amount of idle time"
      def make(
        _param: Param,
        _timer: param.Timer,
        _stats: param.Stats,
        next: ServiceFactory[Req, Rep]
      ) = {
        val param.Timer(timer) = _timer
        val ExpiringService.Param(idleTime, lifeTime) = _param
        val param.Stats(statsReceiver) = _stats

        val idle = if (idleTime.isFinite) Some(idleTime) else None
        val life = if (lifeTime.isFinite) Some(lifeTime) else None

        (idle, life) match {
          case (None, None) => next
          case _ =>
            next.map { service =>
              val closeOnRelease = closeOnReleaseSvc[Req, Rep](service)

              new ExpiringService(closeOnRelease, idle, life, timer, statsReceiver) {
                def onExpire(): Unit = { closeOnRelease.close() }
              }
            }
        }
      }
    }
}

/**
 * A service wrapper that expires the `self` service after a
 * certain amount of idle time. By default, expiring calls
 * `close()` on the `self` channel, but this action is
 * customizable.
 *
 * @see The [[https://twitter.github.io/finagle/guide/Clients.html#timeouts-expiration user guide]]
 *      for more details.
 */
abstract class ExpiringService[Req, Rep](
  self: Service[Req, Rep],
  maxIdleTime: Option[Duration],
  maxLifeTime: Option[Duration],
  timer: Timer,
  stats: StatsReceiver)
    extends ServiceProxy[Req, Rep](self) {
  private[this] var active = true
  private[this] val latch = new AsyncLatch

  private[this] val idleCounter = stats.counter("idle")
  private[this] val lifeCounter = stats.counter("lifetime")
  private[this] var idleTask = startTimer(maxIdleTime, idleCounter)
  private[this] var lifeTask = startTimer(maxLifeTime, lifeCounter)
  private[this] val expireFnCalled = new AtomicBoolean(false)
  private[this] val didExpire = new Promise[Unit]

  private[this] def startTimer(duration: Option[Duration], counter: Counter) =
    duration
      .map { t: Duration => timer.schedule(t.fromNow) { expire(counter) } }
      .getOrElse { NullTimerTask }

  private[this] def expire(counter: Counter): Unit = {
    if (deactivate()) {
      latch.await {
        expired()
        counter.incr()
      }
    }
  }

  private[this] def deactivate(): Boolean = synchronized {
    if (!active) false
    else {
      active = false
      idleTask.cancel()
      lifeTask.cancel()
      idleTask = NullTimerTask
      lifeTask = NullTimerTask
      true
    }
  }

  private[this] def expired(): Unit = {
    if (expireFnCalled.compareAndSet(false, true)) {
      didExpire.setDone()
      onExpire()
    }
  }

  protected def onExpire(): Unit

  override def apply(req: Req): Future[Rep] = {
    val decrLatch = synchronized {
      if (!active) false
      else {
        if (latch.incr() == 1) {
          idleTask.cancel()
          idleTask = NullTimerTask
        }
        true
      }
    }

    super.apply(req).ensure {
      if (decrLatch) {
        val n = latch.decr()
        synchronized {
          if (n == 0 && active)
            idleTask = startTimer(maxIdleTime, idleCounter)
        }
      }
    }
  }

  override def close(deadline: Time): Future[Unit] = {
    deactivate()
    expired()
    didExpire
  }
}
