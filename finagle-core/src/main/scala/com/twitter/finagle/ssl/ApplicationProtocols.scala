package com.twitter.finagle.ssl

/**
 * ApplicationProtocols represent the prioritized list of Application-Layer Protocol
 * Negotiation (ALPN) or Next Protocol Negotiation (NPN) values that a configured TLS
 * [[Engine]] should support.
 */
private[finagle] sealed trait ApplicationProtocols

private[finagle] object ApplicationProtocols {

  /**
   * Indicates that the determination for which values to use for application protocols
   * with the particular engine should be delegated to the engine factory.
   */
  case object Unspecified extends ApplicationProtocols

  /**
   * Indicates that the values specified should be used with the engine factory for
   * ALPN or NPN for the created TLS [[Engine]].
   *
   * @param appProtocols A prioritized list of which protocols the TLS [[Engine]] should
   * support.
   *
   * @note Values are dependent on the particular engine factory as well as potentially
   * the underlying native library.
   *
   * {{{
   *   val protos = ApplicationProtocols.Supported(Seq("h2", "spdy/3.1", "http/1.1"))
   * }}}
   */
  case class Supported(appProtocols: Seq[String]) extends ApplicationProtocols
}
