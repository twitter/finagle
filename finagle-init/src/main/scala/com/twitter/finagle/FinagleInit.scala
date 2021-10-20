package com.twitter.finagle

/**
 * Initialization code to run before `Finagle` bootstraps any resources (such
 * as its scheduler). The only guarantees are that all `FinagleInit` modules run
 * exactly once and they run before any `Finagle` clients or servers within the
 * process connect to a remote peer or accept connections, respectively.
 *
 * @note There are *no* relative ordering guarantees if multiple `FinagleInit` or `MetricsInit`
 *       instances are registered.
 */
trait FinagleInit extends (() => Unit) {

  /**
   * User-friendly label describing the module.
   */
  def label: String
}
