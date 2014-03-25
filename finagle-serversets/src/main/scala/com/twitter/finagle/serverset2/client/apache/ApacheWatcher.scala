package com.twitter.finagle.serverset2.client.apache

import com.twitter.finagle.serverset2.client.{EventDeliveryThread, SessionStats, WatchState}
import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.util.Var
import org.apache.zookeeper.{Watcher, WatchedEvent}

private[serverset2] class ApacheWatcher(statsIn: StatsReceiver = NullStatsReceiver)
    extends Watcher with SessionStats {
  protected val stats = statsIn
  val state = Var[WatchState](WatchState.Pending)
  def process(event: WatchedEvent) = {
    event.getType match {
      case Watcher.Event.EventType.None =>
        EventDeliveryThread.offer(
          state,
          WatchState.SessionState(SessionStateFilter(ApacheSessionState(event.getState))))
      case e =>
        EventDeliveryThread.offer(
          state,
          WatchState.Determined(ApacheNodeEvent(e)))
    }
  }
}
