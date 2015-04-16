package com.twitter.finagle.serverset2.client

import com.twitter.io.Buf

private[serverset2] object Perms {
  val Read: Int   = 1 << 0
  val Write: Int  = 1 << 1
  val Create: Int = 1 << 2
  val Delete: Int = 1 << 3
  val Admin:Int   = 1 << 4
  val All = Read | Write | Create | Delete | Admin
}

private[serverset2] sealed trait CreateMode

private[serverset2] object CreateMode {
  object Persistent extends CreateMode
  object PersistentSequential extends CreateMode
  object Ephemeral extends CreateMode
  object EphemeralSequential extends CreateMode
}

private[serverset2] sealed trait Node {
  val stat: Data.Stat
}

private[serverset2] object Node {
  case class ACL(
      acl: Seq[com.twitter.finagle.serverset2.client.Data.ACL],
      stat: com.twitter.finagle.serverset2.client.Data.Stat)
    extends Node

  case class Children(
      children: Seq[String],
      stat: com.twitter.finagle.serverset2.client.Data.Stat)
    extends Node

  case class Data(data: Option[Buf], stat: com.twitter.finagle.serverset2.client.Data.Stat) extends Node
}

private[serverset2] sealed abstract class NodeEvent(val name: String)

private[serverset2] object NodeEvent {
  object Created extends NodeEvent("node_created")
  object DataChanged extends NodeEvent("node_data_changed")
  object Deleted extends NodeEvent("node_deleted")
  object ChildrenChanged extends NodeEvent("node_children_changed")
  object DataWatchRemoved extends NodeEvent("node_data_watch_removed")
  object ChildWatchRemoved extends NodeEvent("node_child_watch_removed")
}

private[serverset2] sealed trait Op

private[serverset2] object Op {
  case class Create(
      path: String, data: Option[Buf], acl: Seq[Data.ACL], createMode: CreateMode) extends Op
  case class SetData(path: String, data: Option[Buf], version: Option[Int]) extends Op
  case class Delete(path: String, version: Option[Int]) extends Op
  case class Check(path: String, version: Int) extends Op
}

private[serverset2] sealed trait OpResult

private[serverset2] object OpResult {
  case class CreateResult(path: String, stat: Option[Data.Stat]) extends OpResult
  case class SetDataResult(stat: Data.Stat) extends OpResult
  object DeleteResult extends OpResult
  object CheckResult extends OpResult
  case class ErrorResult(exc: KeeperException) extends OpResult
}

private[serverset2] sealed abstract class SessionState(val name: String)

private[serverset2] object SessionState {
  object Unknown extends SessionState("session_unknown")
  object AuthFailed extends SessionState("session_auth_failed")
  object Disconnected extends SessionState("session_disconnected")
  object Expired extends SessionState("session_expired")
  object NoSyncConnected extends SessionState("session_no_sync_connected")
  object SyncConnected extends SessionState("session_sync_connected")
  object ConnectedReadOnly extends SessionState("session_connected_read_only")
  object SaslAuthenticated extends SessionState("session_sasl_authenticated")
}

private[serverset2] sealed trait WatchState

private[serverset2] object WatchState {
  object Pending extends WatchState
  case class Determined(event: NodeEvent) extends WatchState
  case class SessionState(state: com.twitter.finagle.serverset2.client.SessionState) extends WatchState
}
