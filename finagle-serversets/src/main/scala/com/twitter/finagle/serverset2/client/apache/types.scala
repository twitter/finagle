package com.twitter.finagle.serverset2.client.apache

import com.twitter.finagle.serverset2.client.CreateMode
import com.twitter.finagle.serverset2.client.NodeEvent
import com.twitter.finagle.serverset2.client.SessionState
import org.apache.zookeeper.Watcher.Event.EventType
import org.apache.zookeeper.Watcher.Event.KeeperState

private[serverset2] object ApacheCreateMode {
  def zkMap: Map[CreateMode, org.apache.zookeeper.CreateMode] = Map(
    CreateMode.Ephemeral -> org.apache.zookeeper.CreateMode.EPHEMERAL,
    CreateMode.EphemeralSequential -> org.apache.zookeeper.CreateMode.EPHEMERAL_SEQUENTIAL,
    CreateMode.Persistent -> org.apache.zookeeper.CreateMode.PERSISTENT,
    CreateMode.PersistentSequential -> org.apache.zookeeper.CreateMode.PERSISTENT_SEQUENTIAL
  )

  val zk: CreateMode => org.apache.zookeeper.CreateMode = zkMap
}

private[serverset2] object ApacheNodeEvent {
  def map = Map(
    EventType.NodeChildrenChanged -> NodeEvent.ChildrenChanged,
    EventType.NodeCreated -> NodeEvent.Created,
    EventType.NodeDataChanged -> NodeEvent.DataChanged,
    EventType.NodeDeleted -> NodeEvent.Deleted
  )

  def apply(event: EventType): NodeEvent = map(event)
}

private[serverset2] object ApacheSessionState {
  val map = {
    // the "Closed" state may not exist for certain versions of ZK Client, so we handle
    // this special case in order to have compatibility across versions
    val closedState =
      try {
        Map(KeeperState.valueOf("Closed") -> SessionState.Closed)
      } catch {
        case _: NullPointerException | _: IllegalArgumentException => Map.empty
      }

    Map(
      KeeperState.Unknown -> SessionState.Unknown,
      KeeperState.AuthFailed -> SessionState.AuthFailed,
      KeeperState.Disconnected -> SessionState.Disconnected,
      KeeperState.Expired -> SessionState.Expired,
      KeeperState.NoSyncConnected -> SessionState.NoSyncConnected,
      KeeperState.SyncConnected -> SessionState.SyncConnected,
      KeeperState.SaslAuthenticated -> SessionState.SaslAuthenticated,
      KeeperState.ConnectedReadOnly -> SessionState.ConnectedReadOnly
    ) ++ closedState
  }

  def apply(state: KeeperState): SessionState = map(state)
}
