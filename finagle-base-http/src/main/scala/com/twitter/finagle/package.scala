package com.twitter.finagle

import com.twitter.finagle.stats.DefaultStatsReceiver
import com.twitter.finagle.toggle.{StandardToggleMap, ToggleMap}

package object http {

  /**
   * A type alias for finagle.headers.HeaderMap
   */
  type HeaderMap = http.headers.HeaderMap
  /**
   * The companion object for HeaderMap
   */
  val HeaderMap = http.headers.HeaderMap
  val Rfc7230HeaderValidation = http.headers.Rfc7230HeaderValidation

  /**
   * The name of the finagle-http [[ToggleMap]].
   */
  private val LibraryName: String =
    "com.twitter.finagle.http"

  /**
   * The [[ToggleMap]] used for finagle-http.
   */
  private[finagle] val Toggles: ToggleMap =
    StandardToggleMap(LibraryName, DefaultStatsReceiver)
}
