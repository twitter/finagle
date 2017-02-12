package com.twitter.finagle.stats

import com.twitter.finagle.toggle.{StandardToggleMap, ToggleMap}

/**
 * Normally would be a package object, but `util-stats` already has a
 * `com.twitter.finagle.stats` package object that prevents us from
 * using it here.
 */
private[stats] object Stats {

  /**
   * The name of the finagle-stats [[ToggleMap]].
   */
  private val LibraryName: String =
    "com.twitter.finagle.stats"

  /**
   * The [[ToggleMap]] used for finagle-stats.
   */
  val Toggles: ToggleMap =
    StandardToggleMap(LibraryName, DefaultStatsReceiver)

}
