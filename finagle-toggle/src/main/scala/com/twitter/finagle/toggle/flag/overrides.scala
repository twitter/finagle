package com.twitter.finagle.toggle.flag

import com.twitter.app.GlobalFlag

/**
 * `GlobalFlag` source for the Flag-based
 * [[com.twitter.finagle.toggle.ToggleMap ToggleMap]].
 *
 * Usage is of the form:
 * {{{
 * -com.twitter.finagle.toggle.flag.overrides=com.twitter.finagle.NewThing=0.1,com.twitter.finagle.OtherThing=0.99
 * }}}
 *
 * Methods [[overrides$.let(id:String,fraction:Double*]] and [[overrides.letClear]]
 * are available to help writing unit tests that manipulate
 * flag-based `Toggle` values.
 *
 * @see [[com.twitter.finagle.toggle.ToggleMap.flags]]
 */
object overrides
    extends GlobalFlag[Map[String, Double]](
      Map.empty,
      """Source for the Flag-based ToggleMap.
      |Format is `com.yourpackage.id1=fraction1,com.yourpackage.id2=fraction2,...`
      |where fractions must be [0.0-1.0]""".stripMargin
    ) {

  /**
   * Run `f` with the given `Toggles` set to `fraction`.
   */
  def let[R](id: String, fraction: Double)(f: => R): R =
    let(apply() + (id -> fraction))(f)

  /**
   * Run `f` with the given `Toggle` not assigned
   * in the flag-based `ToggleMap`.
   */
  def letClear[R](id: String)(f: => R): R =
    let(apply() - id)(f)

}
