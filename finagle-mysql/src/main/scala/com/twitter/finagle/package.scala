package com.twitter.finagle

import com.twitter.finagle.stats.DefaultStatsReceiver
import com.twitter.finagle.toggle.{StandardToggleMap, ToggleMap}

package object mysql {

  private[this] val LibraryName: String = "com.twitter.finagle.mysql"

  /**
   * The [[ToggleMap]] used for finagle-mysql
   */
  private[finagle] val Toggles: ToggleMap =
    StandardToggleMap(LibraryName, DefaultStatsReceiver)
}
