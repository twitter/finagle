package com.twitter.finagle.service

import com.twitter.conversions.time._
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.util.Proc
import com.twitter.finagle.{
  ClientConnection, ServiceFactory, ServiceFactoryProxy, FailedFastException}
import com.twitter.util.{Future, Duration, Time, Throw, Return, Timer, TimerTask}
import scala.util.Random

private[finagle] object FailFastFactory {
  private sealed trait State
  private case object Ok extends State
  private case class Retrying(since: Time, task: TimerTask, ntries: Int, backoffs: Stream[Duration]) extends State

  private object Observation extends Enumeration {
    type t = Value
    val Success, Fail, Timeout, TimeoutFail, Close = Value
  }

  private val defaultBackoffs = (Backoff.exponential(1.second, 2) take 5) ++ Backoff.const(32.seconds)
  private val rng = new Random

  // This perhaps should be a write exception, but in reality it's
  // only dispatched when all hosts in the cluster are failed, and so
  // we don't want to retry. This is a bit of a kludge--we should
  // reconsider having this logic in the load balancer instead.
  private val failedFastExc = Future.exception(new FailedFastException)
}

/**
 * An experimental fail-fast factory that attempts to reduce
 * the amount of requests dispatched to endpoints that will
 * anyway fail. It works by marking a host dead on failure,
 * launching a background process that attempts to reestablish
 * the connection with the given backoff schedule. At this time,
 * the factory is marked unavailable (and thus the load balancer
 * above it will avoid its use). The factory becomes available
 * again on success or when the backoff schedule runs out.
 */
private[finagle] class FailFastFactory[Req, Rep](
  self: ServiceFactory[Req, Rep],
  statsReceiver: StatsReceiver,
  timer: Timer,
  backoffs: Stream[Duration] = FailFastFactory.defaultBackoffs
) extends ServiceFactoryProxy(self) {
  import FailFastFactory._

  private[this] def getBackoffs(): Stream[Duration] = backoffs map { duration =>
    // Add a 10% jitter to reduce correlation.
    val ms = duration.inMilliseconds
    (ms + ms*(rng.nextFloat()*0.10)).toInt.milliseconds
  }

  @volatile private[this] var state: State = Ok
  private[this] var proc: Proc[Observation.t] = _
  proc = Proc[Observation.t] {
    case Observation.Success if state != Ok =>
      val Retrying(_, task, _, _) = state
      task.cancel()
      state = Ok

    case Observation.Fail if state == Ok =>
      val wait #:: rest = getBackoffs()
      val now = Time.now
      val task = timer.schedule(now + wait) { proc ! Observation.Timeout }
      state = Retrying(now, task, 0, rest)

    case Observation.TimeoutFail if state != Ok =>
      state match {
        case Retrying(_, _, _, Stream.Empty) =>
          state = Ok

        case Retrying(since, _, ntries, wait #:: rest) =>
          val task = timer.schedule(Time.now + wait) { proc ! Observation.Timeout }
          state = Retrying(since, task, ntries+1, rest)

        case Ok => assert(false)
      }

    case Observation.Timeout if state != Ok =>
      self(ClientConnection.nil) respond {
        case Throw(exc) => proc ! Observation.TimeoutFail
        case Return(service) =>
          proc ! Observation.Success
          service.close()
      }

    case Observation.Close =>
      state match {
        case Retrying(_, task, _, _) =>
          task.cancel()
        case _ =>
      }
      state = Ok
      proc.close()

    case _ => ()
  }

  private[this] val unhealthyForMsGauge =
    statsReceiver.addGauge("unhealthy_for_ms") {
      state match {
        case Retrying(since, _, _, _) => since.untilNow.inMilliseconds
        case _ => 0
      }
    }

  private[this] val unhealthyNumRetriesGauge =
    statsReceiver.addGauge("unhealthy_num_tries") {
      state match {
        case Retrying(_, _, ntries, _) => ntries
        case _ => 0
      }
    }

  override def apply(conn: ClientConnection) =
    if (state != Ok) failedFastExc else {
      self(conn) respond {
        case Throw(_) => proc ! Observation.Fail
        case Return(_) if state != Ok => proc ! Observation.Success
        case _ =>
      }
    }

  override def isAvailable = self.isAvailable && state == Ok
  override val toString = "fail_fast_%s".format(self.toString)

  override def close(deadline: Time) = {
    proc ! Observation.Close
    self.close(deadline)
  }
}
