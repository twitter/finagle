package com.twitter.finagle.serverset2.client.apache

import com.twitter.conversions.time._
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.serverset2.client._
import com.twitter.finagle.util.DefaultTimer
import org.apache.zookeeper.WatchedEvent
import org.apache.zookeeper.Watcher.Event.{EventType, KeeperState}
import org.junit.runner.RunWith
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, OneInstancePerTest}

@RunWith(classOf[JUnitRunner])
class ApacheWatcherTest extends FlatSpec
  with OneInstancePerTest
  with Eventually
  with IntegrationPatience {

  val statsReceiver = new InMemoryStatsReceiver
  val watcher = new ApacheWatcher(statsReceiver)

  val path = "/foo"

  val sessionEvents = Map(
    (KeeperState.Unknown, SessionState.Unknown),
    (KeeperState.AuthFailed, SessionState.AuthFailed),
    (KeeperState.Disconnected, SessionState.Disconnected),
    (KeeperState.Expired, SessionState.Expired),
    (KeeperState.NoSyncConnected, SessionState.NoSyncConnected),
    (KeeperState.SyncConnected, SessionState.SyncConnected))

  val nodeEvents = Map(
    (EventType.NodeChildrenChanged, NodeEvent.ChildrenChanged),
    (EventType.NodeCreated, NodeEvent.Created),
    (EventType.NodeDataChanged, NodeEvent.DataChanged),
    (EventType.NodeDeleted, NodeEvent.Deleted))

  "ApacheWatcher" should "start in the pending state" in {
    assert(watcher.state() === WatchState.Pending)
  }

  if(!sys.props.contains("SKIP_FLAKY")) {
    "ApacheWatcher" should "handle session events" in {
      for (ks <- KeeperState.values) {
        watcher.process(new WatchedEvent(EventType.None, ks, path))
        eventually {
          assert(watcher.state() === WatchState.SessionState(sessionEvents(ks)))
        }
    }
  }
  }

  "ApacheWatcher" should "handle and count node events" in {
    for (ev <- EventType.values) {
      watcher.process(new WatchedEvent(ev, KeeperState.SyncConnected, path))
      if (ev == EventType.None) {
        eventually {
          assert(watcher.state() === WatchState.SessionState(SessionState.SyncConnected))
        }
      }
      else {
        eventually {
          assert(watcher.state() === WatchState.Determined(nodeEvents(ev)))
        }
        assert(statsReceiver.counter(ApacheNodeEvent(ev).name)() === 1)
      }
    }
  }

  "StatsWatcher" should "count session events" in {
    val statsWatcher = SessionStats.watcher(watcher.state, statsReceiver, 5.seconds, DefaultTimer.twitter)
    for (ks <- KeeperState.values) {
      watcher.process(new WatchedEvent(EventType.None, ks, path))
      var currentState: WatchState = WatchState.Pending
      statsWatcher.changes respond { s: WatchState =>
        currentState = s
      }
      eventually {
        assert(currentState === WatchState.SessionState(sessionEvents(ks)))
      }
      assert(statsReceiver.counter(ApacheSessionState(ks).name)() === 1)
    }
  }
}
