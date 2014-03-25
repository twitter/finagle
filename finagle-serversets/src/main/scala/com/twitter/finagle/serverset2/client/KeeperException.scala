package com.twitter.finagle.serverset2.client

private[serverset2] sealed trait KeeperException extends Throwable {
  val path: Option[String]
}

private[serverset2] object KeeperException {
  case class APIError(path: Option[String]) extends KeeperException
  case class AuthFailed(path: Option[String]) extends KeeperException
  case class BadArguments(path: Option[String]) extends KeeperException
  case class BadVersion(path: Option[String]) extends KeeperException
  case class ConnectionLoss(path: Option[String]) extends KeeperException
  case class DataInconsistency(path: Option[String]) extends KeeperException
  case class InvalidACL(path: Option[String]) extends KeeperException
  case class InvalidCallback(path: Option[String]) extends KeeperException
  case class MarshallingError(path: Option[String]) extends KeeperException
  case class NoAuth(path: Option[String]) extends KeeperException
  case class NoChildrenForEphemerals(path: Option[String]) extends KeeperException
  case class NodeExists(path: Option[String]) extends KeeperException
  case class NoNode(path: Option[String]) extends KeeperException
  case class NotEmpty(path: Option[String]) extends KeeperException
  case class NoWatcher(path: Option[String]) extends KeeperException
  case class OperationTimeout(path: Option[String]) extends KeeperException
  case class ReadOnly(path: Option[String]) extends KeeperException
  case class RuntimeInconsistency(path: Option[String]) extends KeeperException
  case class SessionExpired(path: Option[String]) extends KeeperException
  case class SessionMoved(path: Option[String]) extends KeeperException
  case class SystemError(path: Option[String]) extends KeeperException
  case class Unimplemented(path: Option[String]) extends KeeperException
  case class UnknownError(path: Option[String]) extends KeeperException
}
