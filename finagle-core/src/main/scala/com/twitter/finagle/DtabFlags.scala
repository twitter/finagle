package com.twitter.finagle

import com.twitter.app.{App, Flag}
import com.twitter.logging.Logger
import scala.util.control.NonFatal

/**
 * Defines a [[com.twitter.app.Flag]] for specifying a supplemental [[com.twitter.finagle.Dtab]]
 * to append to the [[com.twitter.finagle.Dtab.base]] delegation table.
 */
trait DtabFlags { self: App =>

  /**
   * A [[com.twitter.app.Flag]] for appending a [[com.twitter.finagle.Dtab]] to
   * the [[com.twitter.finagle.Dtab.base]].
   *
   * @note The base, or "system", or "global", delegation table applies to every
   *       request in this process.
   * @see [[com.twitter.finagle.Dtab.base]]
   */
  val dtabAddBaseFlag: Flag[Dtab] =
    flag(
      "dtab.add",
      Dtab.empty,
      "Supplemental Dtab to add to the `c.t.finagle.Dtab.base` delegation table.")

  /**
   * Adds the parsed values of the defined Flags to their currently configured tables, e.g.,
   * either [[com.twitter.finagle.Dtab.base]] or [[com.twitter.finagle.Dtab.local]].
   */
  def addDtabs(): Unit = {
    try {
      Dtab.base ++= dtabAddBaseFlag()
    } catch {
      case NonFatal(e) =>
        Logger
          .get(this.getClass).error(
            e,
            "Unable to append supplemental Dtab to the `com.twitter.finagle.Dtab.base` delegation table.")
    }
  }
}
