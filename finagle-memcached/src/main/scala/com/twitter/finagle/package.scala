package com.twitter.finagle

import com.twitter.finagle.stats.DefaultStatsReceiver
import com.twitter.finagle.toggle.{StandardToggleMap, ToggleMap}

package object memcached {

  /**
   * The name of the finagle-memcached [[ToggleMap]].
   */
  private[this] val LibraryName: String =
    "com.twitter.finagle.memcached"

  /**
   * The [[ToggleMap]] used for finagle-memcached.
   */
  private[finagle] val Toggles: ToggleMap =
    StandardToggleMap(LibraryName, DefaultStatsReceiver)
}
