package com.twitter.finagle.serverset2

import com.twitter.finagle.serverset2.ServiceDiscoverer.ClientHealth
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.util._
import java.util.concurrent.atomic.AtomicReference

/**
 * ZooKeeper sessions are resilient to failure. There are various levels of retry
 * functionality built into the stack to handle these expected events. However, all
 * states are reported to listeners making a status of 'unhealthy' temporarily
 * expected and possibly normal. (e.g. Zookeeper connection loadbalancing, rollout,
 * long GC pause on server or client side).
 *
 * The HealthStabilizer takes changing health status and hides 'unhealthy' status
 * changes until the client has been 'unhealthy' for at least one probation period.
 */
private[serverset2] object HealthStabilizer {

  // A superset of ClientHealth used to track probation in addition to healthy/unhealthy
  private sealed trait Status
  private case object Unknown extends Status
  private case object Healthy extends Status
  private case object Unhealthy extends Status
  private case class Probation(timeInProbation: Stopwatch.Elapsed) extends Status

  def apply(
    va: Var[ClientHealth],
    probationEpoch: Epoch,
    statsReceiver: StatsReceiver
  ): Var[ClientHealth] = {

    Var.async[ClientHealth](ClientHealth.Unknown) { u =>
      val stateChanges = va.changes.dedup.select(probationEpoch.event).foldLeft[Status](Unknown) {
        // always take the first update as our status
        case (Unknown, Left(ClientHealth.Healthy)) => Healthy
        case (Unknown, Left(ClientHealth.Unhealthy)) => Unhealthy

        // Any change from * => healthy makes us immediately healthy
        case (_, Left(ClientHealth.Healthy)) => Healthy

        // Change from good to bad is placed in limbo starting now
        case (Healthy, Left(ClientHealth.Unhealthy)) =>
          Probation(Stopwatch.start())

        // The probation epoch has ended. If we entered probation > the probation duration then we are
        // now unhealthy.
        case (Probation(elapsed), Right(())) if elapsed() >= probationEpoch.period =>
          Unhealthy

        // any other change is ignored
        case (v, _) => v
      }

      val currentStatus = new AtomicReference[Status]()
      val gaugeListener = stateChanges.dedup.register(Witness { currentStatus })
      // scalafix:off StoreGaugesAsMemberVariables
      val gauge = statsReceiver.addGauge("zkHealth") {
        currentStatus.get() match {
          case Unknown => 0
          case Healthy => 1
          case Unhealthy => 2
          case Probation(_) => 3
        }
      }
      // scalafix:on StoreGaugesAsMemberVariables

      val notify = stateChanges
        .collect {
          // re-map to the underlying health status
          case Healthy | Probation(_) => ClientHealth.Healthy
          case Unhealthy => ClientHealth.Unhealthy
          case Unknown => ClientHealth.Unknown
        }
        .dedup
        .register(Witness(u))

      Closable.all(
        notify,
        gaugeListener,
        Closable.make { _ =>
          gauge.remove()
          Future.Done
        })
    }
  }
}
