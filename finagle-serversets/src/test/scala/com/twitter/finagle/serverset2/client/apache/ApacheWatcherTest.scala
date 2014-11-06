package com.twitter.finagle.serverset2.client.apache

import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.serverset2.client._
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
    assert(statsReceiver.counter("session_auth_failures")() === 1)
    assert(statsReceiver.counter("session_connects")() === 1)
    assert(statsReceiver.counter("session_disconnects")() === 1)
    assert(statsReceiver.counter("session_expirations")() === 1)
  }
  }

  "ApacheWatcher" should "handle node events" in {
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
      }
    }
  }
}
