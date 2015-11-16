package com.twitter.finagle.serverset2.client.apache

import com.twitter.conversions.time._
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.serverset2.client._
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util.Await
import org.apache.zookeeper.WatchedEvent
import org.apache.zookeeper.Watcher.Event.{EventType, KeeperState}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, OneInstancePerTest}

@RunWith(classOf[JUnitRunner])
class ApacheWatcherTest extends FlatSpec
  with OneInstancePerTest {

  val statsReceiver = new InMemoryStatsReceiver
  val watcher = new ApacheWatcher(statsReceiver)

  val path = "/foo"

  val sessionEvents = Map(
    (KeeperState.Unknown, SessionState.Unknown),
    (KeeperState.AuthFailed, SessionState.AuthFailed),
    (KeeperState.Disconnected, SessionState.Disconnected),
    (KeeperState.Expired, SessionState.Expired),
    (KeeperState.NoSyncConnected, SessionState.NoSyncConnected),
    (KeeperState.SyncConnected, SessionState.SyncConnected),
    (KeeperState.SaslAuthenticated, SessionState.SaslAuthenticated),
    (KeeperState.ConnectedReadOnly, SessionState.ConnectedReadOnly))

  val nodeEvents = Map(
    (EventType.NodeChildrenChanged, NodeEvent.ChildrenChanged),
    (EventType.NodeCreated, NodeEvent.Created),
    (EventType.NodeDataChanged, NodeEvent.DataChanged),
    (EventType.NodeDeleted, NodeEvent.Deleted))

  "ApacheWatcher" should "start in the pending state" in {
    assert(watcher.state() == WatchState.Pending)
  }

  "ApacheWatcher" should "handle session events" in {
    for (ks <- sessionEvents.keys) {
      val satisfied = watcher.state.changes.filter(_ == WatchState.SessionState(sessionEvents(ks))).toFuture
      watcher.process(new WatchedEvent(EventType.None, ks, path))
      assert(Await.result(satisfied) == WatchState.SessionState(sessionEvents(ks)))
    }
  }

  "ApacheWatcher" should "handle and count node events" in {
    for (ev <- nodeEvents.keys) {
      if (ev != EventType.None) {
        val determined = watcher.state.changes.filter(_ == WatchState.Determined(nodeEvents(ev))).toFuture
        watcher.process(new WatchedEvent(ev, KeeperState.SyncConnected, path))
        assert(Await.result(determined) == WatchState.Determined(nodeEvents(ev)))
        assert(statsReceiver.counter(ApacheNodeEvent(ev).name)() == 1)      }
    }
  }

  "StatsWatcher" should "count session events" in {
    val statsWatcher = SessionStats.watcher(watcher.state, statsReceiver, 5.seconds, DefaultTimer.twitter)
    // Set a constant witness so the Var doesn't reset state
    statsWatcher.changes.respond(_ => ())
    for (ks <- KeeperState.values) {
      val satisfied = statsWatcher.changes.filter(_ == WatchState.SessionState(sessionEvents(ks))).toFuture
      watcher.process(new WatchedEvent(EventType.None, ks, path))
      assert(Await.result(satisfied) == WatchState.SessionState(sessionEvents(ks)))
      assert(statsReceiver.counter(ApacheSessionState(ks).name)() == 1)
    }
  }
}
