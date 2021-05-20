package com.twitter.finagle.serverset2.client.apache

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.serverset2.client._
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util.Await
import org.apache.zookeeper.WatchedEvent
import org.apache.zookeeper.Watcher.Event.{EventType, KeeperState}
import org.scalatest.OneInstancePerTest
import org.scalatest.funsuite.AnyFunSuite

class ApacheWatcherTest extends AnyFunSuite with OneInstancePerTest {

  val statsReceiver = new InMemoryStatsReceiver
  val watcher = new ApacheWatcher(statsReceiver)

  val path = "/foo"

  // the "Closed" state may not exist for certain versions of ZK Client, so we handle
  // this special case in order to have compatibility across versions
  val closedKeeperState =
    try {
      Map(KeeperState.valueOf("Closed") -> SessionState.Closed)
    } catch {
      case _: IllegalArgumentException => Map.empty
    }

  val sessionEvents = Map(
    (KeeperState.Unknown, SessionState.Unknown),
    (KeeperState.AuthFailed, SessionState.AuthFailed),
    (KeeperState.Disconnected, SessionState.Disconnected),
    (KeeperState.Expired, SessionState.Expired),
    (KeeperState.NoSyncConnected, SessionState.NoSyncConnected),
    (KeeperState.SyncConnected, SessionState.SyncConnected),
    (KeeperState.SaslAuthenticated, SessionState.SaslAuthenticated),
    (KeeperState.ConnectedReadOnly, SessionState.ConnectedReadOnly)
  ) ++ closedKeeperState

  val nodeEvents = Map(
    (EventType.NodeChildrenChanged, NodeEvent.ChildrenChanged),
    (EventType.NodeCreated, NodeEvent.Created),
    (EventType.NodeDataChanged, NodeEvent.DataChanged),
    (EventType.NodeDeleted, NodeEvent.Deleted)
  )

  test("ApacheWatcher starts in the pending state") {
    assert(watcher.state() == WatchState.Pending)
  }

  test("ApacheWatcher handles session events") {
    for (ks <- sessionEvents.keys) {
      val satisfied =
        watcher.state.changes.filter(_ == WatchState.SessionState(sessionEvents(ks))).toFuture
      watcher.process(new WatchedEvent(EventType.None, ks, path))
      assert(Await.result(satisfied, 1.second) == WatchState.SessionState(sessionEvents(ks)))
    }
  }

  test("ApacheWatcher handles and counts node events") {
    for (ev <- nodeEvents.keys) {
      if (ev != EventType.None) {
        val determined =
          watcher.state.changes.filter(_ == WatchState.Determined(nodeEvents(ev))).toFuture
        watcher.process(new WatchedEvent(ev, KeeperState.SyncConnected, path))
        assert(Await.result(determined, 1.second) == WatchState.Determined(nodeEvents(ev)))
        assert(statsReceiver.counter(ApacheNodeEvent(ev).name)() == 1)
      }
    }
  }

  test("StatsWatcher counts session events") {
    val statsWatcher = SessionStats.watcher(watcher.state, statsReceiver, 5.seconds, DefaultTimer)
    // Set a constant witness so the Var doesn't reset state
    statsWatcher.changes.respond(_ => ())
    for (ks <- KeeperState.values) {
      val satisfied =
        statsWatcher.changes.filter(_ == WatchState.SessionState(sessionEvents(ks))).toFuture
      watcher.process(new WatchedEvent(EventType.None, ks, path))
      assert(Await.result(satisfied, 1.second) == WatchState.SessionState(sessionEvents(ks)))
      assert(statsReceiver.counter(ApacheSessionState(ks).name)() == 1)
    }
  }
}
