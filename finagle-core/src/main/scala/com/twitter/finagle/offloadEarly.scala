package com.twitter.finagle

import com.twitter.finagle.server.ServerInfo

/**
 * A toggle that determines whether or not offload happens early in the stack.
 */
private object offloadEarly {
  private val toggle = CoreToggles("com.twitter.finagle.OffloadEarly")
  def apply(): Boolean = toggle(ServerInfo().id.hashCode)
}
