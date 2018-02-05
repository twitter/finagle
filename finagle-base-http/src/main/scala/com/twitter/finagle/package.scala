package com.twitter.finagle

import com.twitter.app.GlobalFlag
import com.twitter.finagle.stats.DefaultStatsReceiver
import com.twitter.finagle.toggle.{StandardToggleMap, ToggleMap}

package object http {

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

  object serverErrorsAsFailures
    extends GlobalFlag(
      true,
      "Treat responses with status codes in " +
        "the 500s as failures. See " +
        "`com.twitter.finagle.http.service.HttpResponseClassifier.ServerErrorsAsFailures`"
    )
}
