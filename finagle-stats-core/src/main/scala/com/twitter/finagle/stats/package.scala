package com.twitter.finagle

import com.twitter.finagle.toggle.StandardToggleMap
import com.twitter.finagle.tunable.StandardTunableMap
import com.twitter.util.tunable.Tunable
import com.twitter.util.tunable.TunableMap

package object stats {

  /**
   * The [[TunableMap]] used within Finagle (currently only needed for finagle-stats).
   */
  private[stats] val Tunables: TunableMap = StandardTunableMap("finagle")

  /**
   * Comma-separated list of *-wildcard expressions to allowlist debug metrics (not exported by
   * default).
   *
   * Example:
   *
   * {{{
   *   foo/bar*,*baz/qux,*aux*
   * }}}
   */
  private[stats] val Verbose: Tunable[String] =
    Tunables(TunableMap.Key[String]("com.twitter.finagle.stats.verbose"))

  private[stats] val Toggles = StandardToggleMap("com.twitter.finagle.stats", DefaultStatsReceiver)
}
