package com.twitter.finagle

import com.twitter.finagle.stats.DefaultStatsReceiver
import com.twitter.finagle.toggle.{StandardToggleMap, ToggleMap}

package object kestrel {

  private[this] val LibraryName: String = "com.twitter.finagle.kestrel"

  /**
   * The [[ToggleMap]] used for finagle-kestrel
   */
  private[finagle] val Toggles: ToggleMap =
    StandardToggleMap(LibraryName, DefaultStatsReceiver)
}