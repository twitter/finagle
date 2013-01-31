package com.twitter.finagle

import java.net.SocketAddress

/**
 * Clients connect to groups.
 */
private[finagle]  // for now
trait Client[Req, Rep] {

  /**
   * Create a new `ServiceFactory` that is connected to `group`.
   */
  def newClient(group: Group[SocketAddress]): ServiceFactory[Req, Rep]

  // TODO: use actual resolver. The current implementation is but a
  // temporary convenience.
  def newClient(dest: String): ServiceFactory[Req, Rep] =
    newClient(Group(dest))
}
