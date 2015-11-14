package com.twitter.finagle.service

import com.twitter.conversions.time._
import com.twitter.finagle._
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.util.{DefaultLogger, Updater}
import com.twitter.finagle.util.InetSocketAddressUtil.unconnected
import com.twitter.logging.Level
import com.twitter.util.{Future, Duration, Time, Throw, Return, Timer, TimerTask, Promise}
import java.net.SocketAddress
import java.util.logging.Logger
import scala.util.Random

object FailFastFactory {
  private sealed trait State
  private case object Ok extends State
  private case class Retrying(
      since: Time,
      task: TimerTask,
      ntries: Int,
      backoffs: Stream[Duration])
    extends State

  private val url = "https://twitter.github.io/finagle/guide/FAQ.html#why-do-clients-see-com-twitter-finagle-failedfastexception-s"

  private object Observation extends Enumeration {
    type t = Value
    val Success, Fail, Timeout, TimeoutFail, Close = Value
  }

  private val defaultBackoffs = (Backoff.exponential(1.second, 2) take 5) ++ Backoff.const(32.seconds)
  private val rng = new Random

  val role = Stack.Role("FailFast")

  /**
   * For details on usage see the
   * [[https://twitter.github.io/finagle/guide/FAQ.html#why-do-clients-see-com-twitter-finagle-failedfastexception-s FAQ]]
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
    new Stack.Module6[
      FailFast,
      param.Stats,
      param.Timer,
      param.Label,
      param.Logger,
      Transporter.EndpointAddr,
      ServiceFactory[Req, Rep]] {
      val role = FailFastFactory.role
      val description = "Backoff exponentially from hosts to which we cannot establish a connection"
      def make(
        failFast: FailFast,
        _stats: param.Stats,
        _timer: param.Timer,
        _label: param.Label,
        _logger: param.Logger,
        _endpoint: Transporter.EndpointAddr,
        next: ServiceFactory[Req, Rep]
      ) = {
        failFast match {
          case FailFast(false) =>
            next
          case FailFast(true) =>
            val param.Stats(statsReceiver) = _stats
            val param.Timer(timer) = _timer
            val param.Label(label) = _label
            val param.Logger(logger) = _logger
            val Transporter.EndpointAddr(endpoint) = _endpoint

            new FailFastFactory(next, statsReceiver.scope("failfast"), timer, label, logger, endpoint)
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
 */
private[finagle] class FailFastFactory[Req, Rep](
    underlying: ServiceFactory[Req, Rep],
    statsReceiver: StatsReceiver,
    timer: Timer,
    label: String,
    logger: Logger = DefaultLogger,
    endpoint: SocketAddress = unconnected,
    backoffs: Stream[Duration] = FailFastFactory.defaultBackoffs)
  extends ServiceFactoryProxy(underlying) {
  import FailFastFactory._

  private[this] val exc = new FailedFastException(s"Endpoint $label is marked down. For more details see: $url")

  private[this] val futureExc = Future.exception(exc)

  private[this] val markedAvailableCounter = statsReceiver.counter("marked_available")
  private[this] val markedDeadCounter = statsReceiver.counter("marked_dead")

  private[this] val unhealthyForMsGauge =
    statsReceiver.addGauge("unhealthy_for_ms") {
      state match {
        case r: Retrying => r.since.untilNow.inMilliseconds
        case _ => 0
      }
    }

  private[this] val unhealthyNumRetriesGauge =
    statsReceiver.addGauge("unhealthy_num_tries") {
      state match {
        case r: Retrying => r.ntries
        case _ => 0
      }
    }

  private[this] def getBackoffs(): Stream[Duration] = backoffs map { duration =>
    // Add a 10% jitter to reduce correlation.
    val ms = duration.inMilliseconds
    (ms + ms*(rng.nextFloat()*0.10)).toInt.milliseconds
  }

  @volatile private[this] var state: State = Ok

  private[this] val update = new Updater[Observation.t] {
    def preprocess(elems: Seq[Observation.t]) = elems

    def handle(o: Observation.t) = o match {
      case Observation.Success if state != Ok =>
        val Retrying(_, task, _, _) = state
        task.cancel()
        markedAvailableCounter.incr()
        state = Ok

      case Observation.Fail if state == Ok =>
        val (wait, rest) = getBackoffs() match {
          case Stream.Empty => (Duration.Zero, Stream.empty[Duration])
          case wait #:: rest => (wait, rest)
        }
        val now = Time.now
        val task = timer.schedule(now + wait) { this.apply(Observation.Timeout) }
        markedDeadCounter.incr()

        if (logger.isLoggable(Level.DEBUG))
          logger.log(Level.DEBUG, s"""FailFastFactory marking connection to "$label" as dead. Remote Address: ${endpoint.toString}""")

        state = Retrying(now, task, 0, rest)

      case Observation.Timeout if state != Ok =>
        underlying(ClientConnection.nil).respond {
          case Throw(_) => this.apply(Observation.TimeoutFail)
          case Return(service) =>
            this.apply(Observation.Success)
            service.close()
        }

      case Observation.TimeoutFail if state != Ok =>
        state match {
          case Retrying(_, task, _, Stream.Empty) =>
            task.cancel()
            // Backoff schedule exhausted. Optimistically become available in
            // order to continue trying.
            state = Ok

          case Retrying(since, task, ntries, wait #:: rest) =>
            task.cancel()
            val newTask = timer.schedule(Time.now + wait) { this.apply(Observation.Timeout) }
            state = Retrying(since, newTask, ntries+1, rest)

          case Ok => assert(false)
        }

      case Observation.Close =>
        val oldState = state
        state = Ok
        oldState match {
          case Retrying(_, task, _, _) =>
            task.cancel()
          case _ =>
        }

      case _ => ()

    }
  }

  override def apply(conn: ClientConnection): Future[Service[Req, Rep]] = {
    if (state != Ok) futureExc else {
      underlying(conn).respond {
        case Throw(_) => update(Observation.Fail)
        case Return(_) if state != Ok => update(Observation.Success)
        case _ =>
      }
    }
  }
  override def status = state match {
    case Ok => underlying.status
    case _: Retrying => Status.Busy
  }

  override val toString = "fail_fast_%s".format(underlying.toString)

  override def close(deadline: Time): Future[Unit] = {
    update(Observation.Close)
    underlying.close(deadline)
  }
}
