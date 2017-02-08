package com.twitter.finagle.ssl

/**
 * Protocols represent the versions of the TLS protocol which should
 * be enabled with a given TLS [[Engine]].
 *
 * @note Java users: See [[ProtocolsConfig]].
 */
sealed trait Protocols

object Protocols {

  /**
   * Indicates that the determination for which TLS protocols are supported
   * should be delegated to the engine factory.
   */
  case object Unspecified extends Protocols

  /**
   * Indicates that only these specific protocols should be enabled for
   * a particular engine.
   *
   * @param protocols A list of protocols which should be enabled for a
   * particular engine. The set of protocols in this list must be a subset
   * of the set of protocols supported by the underlying engine.
   *
   * {{{
   *   val protocols = Protocols.Enabled(Seq("TLSv1.2"))
   * }}}
   */
  case class Enabled(protocols: Seq[String]) extends Protocols
}
