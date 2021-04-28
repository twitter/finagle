package com.twitter.finagle.service

import com.twitter.conversions.DurationOps._
import com.twitter.finagle._
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.util.{DefaultLogger, Updater}
import com.twitter.logging.Level
import com.twitter.util._
import java.util.logging.Logger

object FailFastFactory {
  private sealed trait State
  private case object Ok extends State
  private case class Retrying(
    lastFailure: Future[Nothing],
    since: Time,
    task: TimerTask,
    ntries: Int,
    backoffs: Backoff)
      extends State

  private val url =
    "https://twitter.github.io/finagle/guide/FAQ.html#why-do-clients-see-com-twitter-finagle-failedfastexception-s"

  private sealed trait Observation
  private object Observation {
    final case class Fail(failure: Throwable) extends Observation
    final case class TimeoutFail(failure: Throwable) extends Observation
    object Timeout extends Observation
    object Close extends Observation
    object Success extends Observation
  }

  // put a reasonably sized cap on the number of jittered backoffs so that a
  // "permanently" dead host doesn't create a space leak. since each new backoff
  // that is taken will be held onto by this global Stream (having the trailing
  // `constant` avoids this issue).
  private val defaultBackoffs: Backoff =
    Backoff.exponentialJittered(1.second, 32.seconds).take(16).concat(Backoff.constant(32.seconds))

  val role = Stack.Role("FailFast")

  /**
   * For details on why clients see [[FailedFastException]]s see the
   * [[https://twitter.github.io/finagle/guide/FAQ.html#why-do-clients-see-com-twitter-finagle-failedfastexception-s FAQ]]
   *
   * @see The [[https://twitter.github.io/finagle/guide/Clients.html#fail-fast user guide]]
   *      for more details.
   */
  case class FailFast(enabled: Boolean) {
    def mk(): (FailFast, Stack.Param[FailFast]) =
      (this, FailFast.param)
  }
  object FailFast {
    implicit val param = Stack.Param(FailFast(enabled = true))
  }

  /**
   * Creates a [[com.twitter.finagle.Stackable]] [[FailFastFactory]] when enabled.
   */
  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module5[
      FailFast,
      param.Stats,
      param.Timer,
      param.Label,
      Transporter.EndpointAddr,
      ServiceFactory[Req, Rep]
    ] {
      val role: Stack.Role = FailFastFactory.role
      val description = "Backoff exponentially from hosts to which we cannot establish a connection"
      def make(
        failFast: FailFast,
        _stats: param.Stats,
        _timer: param.Timer,
        _label: param.Label,
        _endpoint: Transporter.EndpointAddr,
        next: ServiceFactory[Req, Rep]
      ): ServiceFactory[Req, Rep] = {
        failFast match {
          case FailFast(false) =>
            next
          case FailFast(true) =>
            val param.Stats(statsReceiver) = _stats
            val param.Timer(timer) = _timer
            val param.Label(label) = _label
            val Transporter.EndpointAddr(endpoint) = _endpoint

            new FailFastFactory(
              next,
              statsReceiver.scope("failfast"),
              timer,
              label,
              DefaultLogger,
              endpoint
            )
        }
      }
    }
}

/**
 * A fail-fast factory that attempts to reduce the number of requests dispatched
 * to endpoints that will anyway fail. It works by marking a host dead on
 * failure, launching a background process that attempts to reestablish the
 * connection with the given backoff schedule. At this time, the factory is
 * marked unavailable (and thus the load balancer above it will avoid its
 * use). The factory becomes available again on success or when the backoff
 * schedule runs out.
 *
 * Inflight attempts to connect will continue uninterrupted. However, trying to
 * connect *after* being marked dead will fail fast until the background process
 * is able to establish a connection.
 *
 * @see The [[https://twitter.github.io/finagle/guide/Clients.html#fail-fast user guide]]
 *      for more details.
 */
private[finagle] class FailFastFactory[Req, Rep](
  underlying: ServiceFactory[Req, Rep],
  statsReceiver: StatsReceiver,
  timer: Timer,
  label: String,
  logger: Logger = DefaultLogger,
  endpoint: Address = Address.failing,
  backoffs: Backoff = FailFastFactory.defaultBackoffs)
    extends ServiceFactoryProxy(underlying) {
  import FailFastFactory._

  @volatile private[this] var state: State = Ok

  private[this] final def failWith(t: Throwable): Future[Nothing] = Future.exception(
    new FailedFastException(s"Endpoint $label is marked down. For more details see: $url", t)
  )

  private[this] val markedAvailableCounter = statsReceiver.counter("marked_available")
  private[this] val markedDeadCounter = statsReceiver.counter("marked_dead")

  private[this] val gauges = Seq(
    statsReceiver.addGauge("unhealthy_for_ms") {
      state match {
        case r: Retrying => r.since.untilNow.inMilliseconds
        case _ => 0
      }
    },
    statsReceiver.addGauge("unhealthy_num_tries") {
      state match {
        case r: Retrying => r.ntries
        case _ => 0
      }
    },
    statsReceiver.addGauge("is_marked_dead") {
      state match {
        case _: Retrying => 1
        case _ => 0
      }
    }
  )

  private[this] val update = new Updater[Observation] {
    def preprocess(elems: Seq[Observation]): Seq[Observation] = elems

    def handle(o: Observation): Unit = o match {
      case Observation.Success if state != Ok =>
        val Retrying(_, _, task, _, _) = state
        task.cancel()
        markedAvailableCounter.incr()
        state = Ok

      case Observation.Fail(failure) if state == Ok =>
        val (wait, rest) =
          if (backoffs.isExhausted) (Duration.Zero, Backoff.empty)
          else (backoffs.duration, backoffs.next)

        val now = Time.now
        val task = timer.schedule(now + wait) { this.apply(Observation.Timeout) }
        markedDeadCounter.incr()

        if (logger.isLoggable(Level.DEBUG))
          logger.log(
            Level.DEBUG,
            s"""FailFastFactory marking connection to "$label" as dead. Remote Address: ${endpoint.toString}"""
          )

        state = Retrying(failWith(failure), now, task, 0, rest)

      case Observation.Timeout if state != Ok =>
        underlying(ClientConnection.nil).respond {
          case Throw(t) => this.apply(Observation.TimeoutFail(t))
          case Return(service) =>
            this.apply(Observation.Success)
            service.close()
        }

      case Observation.TimeoutFail(failure) if state != Ok =>
        state match {
          case Retrying(_, _, task, _, backoffs) if backoffs.isExhausted =>
            task.cancel()
            // Backoff schedule exhausted. Optimistically become available in
            // order to continue trying.
            state = Ok

          case Retrying(_, since, task, ntries, backoffs) =>
            task.cancel()
            val newTask = timer.schedule(Time.now + backoffs.duration) {
              this.apply(Observation.Timeout)
            }
            state = Retrying(failWith(failure), since, newTask, ntries + 1, backoffs.next)

          case Ok => assert(false)
        }

      case Observation.Close =>
        val oldState = state
        state = Ok
        oldState match {
          case Retrying(_, _, task, _, _) =>
            task.cancel()
          case _ =>
        }

      case _ => ()

    }
  }

  override def apply(conn: ClientConnection): Future[Service[Req, Rep]] = state match {
    case Retrying(lastFailure, _, _, _, _) => lastFailure
    case Ok =>
      underlying(conn).respond {
        case Throw(t) => update(Observation.Fail(t))
        case Return(_) if state != Ok => update(Observation.Success)
        case _ =>
      }
  }

  override def status: Status = state match {
    case Ok => underlying.status
    case _: Retrying => Status.Busy
  }

  override val toString: String = s"fail_fast_$underlying"

  override def close(deadline: Time): Future[Unit] = {
    gauges.foreach(_.remove())
    update(Observation.Close)
    underlying.close(deadline)
  }
}
