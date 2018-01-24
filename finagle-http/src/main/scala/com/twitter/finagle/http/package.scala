package com.twitter.finagle

import com.twitter.app.GlobalFlag
import com.twitter.finagle.netty4.http.Netty4CookieCodec
import com.twitter.finagle.server.ServerInfo
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


  private[http] val UseNetty4CookieCodec =
    Toggles("com.twitter.finagle.http.UseNetty4CookieCodec")

  if (UseNetty4CookieCodec(ServerInfo().id.hashCode())) {
    CookieMap.setCookieCodec(Netty4CookieCodec)
  }

  object serverErrorsAsFailures
      extends GlobalFlag(
        true,
        "Treat responses with status codes in " +
          "the 500s as failures. See " +
          "`com.twitter.finagle.http.service.HttpResponseClassifier.ServerErrorsAsFailures`"
      )
}
