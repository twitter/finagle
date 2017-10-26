package com.twitter.finagle

import com.twitter.finagle.stats.DefaultStatsReceiver
import com.twitter.finagle.toggle.{StandardToggleMap, ToggleMap}

package object thriftmux {

  /**
   * The name of the finagle-thriftmux [[ToggleMap]].
   */
  private[this] val LibraryName: String =
    "com.twitter.finagle.thriftmux"

  /**
   * The [[ToggleMap]] used for finagle-thriftmux.
   */
  private[finagle] val Toggles: ToggleMap =
    StandardToggleMap(LibraryName, DefaultStatsReceiver)
}
