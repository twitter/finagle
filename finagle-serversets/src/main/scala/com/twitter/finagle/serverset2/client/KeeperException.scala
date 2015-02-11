package com.twitter.finagle.serverset2.client

private[serverset2] sealed abstract class KeeperException(val name: String) extends Throwable {
  val path: Option[String]
}

private[serverset2] object KeeperException {
  case class APIError(path: Option[String]) extends KeeperException("api_error")
  case class AuthFailed(path: Option[String]) extends KeeperException("auth_failed")
  case class BadArguments(path: Option[String]) extends KeeperException("bad_arguments")
  case class BadVersion(path: Option[String]) extends KeeperException("bad_version")
  case class ConnectionLoss(path: Option[String]) extends KeeperException("connection_loss")
  case class DataInconsistency(path: Option[String]) extends KeeperException("data_inconsistency")
  case class EphemeralOnLocalSession(path: Option[String]) extends KeeperException("ephemeral_on_local_session")
  case class InvalidACL(path: Option[String]) extends KeeperException("invalid_acl")
  case class InvalidCallback(path: Option[String]) extends KeeperException("invalid_callback")
  case class MarshallingError(path: Option[String]) extends KeeperException("marshalling_error")
  case class NewConfigNoQuorum(path: Option[String]) extends KeeperException("new_config_no_quorum")
  case class NoAuth(path: Option[String]) extends KeeperException("no_auth")
  case class NoChildrenForEphemerals(path: Option[String]) extends KeeperException("no_children_for_ephemerals")
  case class NodeExists(path: Option[String]) extends KeeperException("node_exists")
  case class NoNode(path: Option[String]) extends KeeperException("no_node")
  case class NotEmpty(path: Option[String]) extends KeeperException("not_empty")
  case class NoWatcher(path: Option[String]) extends KeeperException("no_watcher")
  case class OperationTimeout(path: Option[String]) extends KeeperException("operation_timeout")
  case class ReadOnly(path: Option[String]) extends KeeperException("read_only")
  case class ReconfigInProgress(path: Option[String]) extends KeeperException("reconfig_in_progress")
  case class RuntimeInconsistency(path: Option[String]) extends KeeperException("runtime_inconsistency")
  case class SessionExpired(path: Option[String]) extends KeeperException("session_expired")
  case class SessionMoved(path: Option[String]) extends KeeperException("session_moved")
  case class SystemError(path: Option[String]) extends KeeperException("system_error")
  case class Unimplemented(path: Option[String]) extends KeeperException("unimplemented")
  case class UnknownError(path: Option[String]) extends KeeperException("unknown_error")
}
