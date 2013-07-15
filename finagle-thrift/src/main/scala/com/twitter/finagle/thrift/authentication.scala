package com.twitter.finagle.thrift

import com.twitter.util.Local

case class ClientId(name: String) {
  private[thrift] def toThrift = {
    val clientId = new thrift.ClientId
    clientId.setName(name)
    clientId
  }

  /**
   * Executes the given function with this ClientId set as the current
   * ClientId.  The current ClientId before executing this will be restored
   * on completion.
   */
  def asCurrent[T](f: => T): T = {
    val old = ClientId.current
    ClientId.set(Some(this))
    try f finally { ClientId.set(old) }
  }
}

/**
 * `ClientId` provides the client identification of the incoming request if available.
 * It is set at the beginning of the request and is available throughout the life-cycle
 * of the request. It is iff the client has an upgraded finagle connection and has chosen
 * to specify the client ID in their codec.
 */
object ClientId {
  private[this] val _current = new Local[ClientId]
  def current = _current()

  private[thrift] def set(clientId: Option[ClientId]) {
    clientId match {
      case Some(id) => _current.update(id)
      case None => _current.clear()
    }
  }

  private[thrift] def clear() = _current.clear()
}
