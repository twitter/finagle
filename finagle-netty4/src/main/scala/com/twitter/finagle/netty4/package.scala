package com.twitter.finagle

import com.twitter.finagle.stats.DefaultStatsReceiver
import com.twitter.finagle.toggle.{StandardToggleMap, ToggleMap}

/**
 * Package netty4 implements the bottom finagle primitives:
 * [[com.twitter.finagle.Server]] and a client transport in terms of
 * the netty4 event loop.
 */
package object netty4 {

  /**
   * The [[ToggleMap]] used for finagle-netty4.
   */
  private[finagle] val Toggles: ToggleMap =
    StandardToggleMap("com.twitter.finagle.netty4", DefaultStatsReceiver)
}
