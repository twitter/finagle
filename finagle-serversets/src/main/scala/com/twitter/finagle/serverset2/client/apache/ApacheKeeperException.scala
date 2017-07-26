package com.twitter.finagle.serverset2.client.apache

import com.twitter.finagle.serverset2.client.KeeperException

private[serverset2] object ApacheKeeperException {

  /**
   * Transform ZooKeeper Error Codes to KeeperExceptions
   *
   * @param err error code
   * @param path path associated with exception. May be null
   * @return Some(KeeperException) or None if no error is raised (OK)
   */
  def apply(err: Int, path: Option[String]): Option[KeeperException] = err match {
    case 0 => None
    case -1 => Some(KeeperException.SystemError(path))
    case -2 => Some(KeeperException.RuntimeInconsistency(path))
    case -3 => Some(KeeperException.DataInconsistency(path))
    case -4 => Some(KeeperException.ConnectionLoss(path))
    case -5 => Some(KeeperException.MarshallingError(path))
    case -6 => Some(KeeperException.Unimplemented(path))
    case -7 => Some(KeeperException.OperationTimeout(path))
    case -8 => Some(KeeperException.BadArguments(path))
    case -100 => Some(KeeperException.APIError(path))
    case -101 => Some(KeeperException.NoNode(path))
    case -102 => Some(KeeperException.NoAuth(path))
    case -103 => Some(KeeperException.BadVersion(path))
    case -108 => Some(KeeperException.NoChildrenForEphemerals(path))
    case -110 => Some(KeeperException.NodeExists(path))
    case -111 => Some(KeeperException.NotEmpty(path))
    case -112 => Some(KeeperException.SessionExpired(path))
    case -113 => Some(KeeperException.InvalidCallback(path))
    case -114 => Some(KeeperException.InvalidACL(path))
    case -115 => Some(KeeperException.AuthFailed(path))
    case -118 => Some(KeeperException.SessionMoved(path))
    case -119 => Some(KeeperException.ReadOnly(path))
    case -120 => Some(KeeperException.NewConfigNoQuorum(path))
    case -121 => Some(KeeperException.ReconfigInProgress(path))
    case -122 => Some(KeeperException.EphemeralOnLocalSession(path))
    case -123 => Some(KeeperException.NoWatcher(path))
    case _ => Some(KeeperException.UnknownError(path))
  }
}
