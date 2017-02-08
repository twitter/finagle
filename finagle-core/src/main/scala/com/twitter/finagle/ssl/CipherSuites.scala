package com.twitter.finagle.ssl

/**
 * CipherSuites represent the collection of prioritized cipher suites that should be
 * enabled for a TLS [[Engine]]. A cipher suite, for protocols prior to TLSv1.3, is a
 * combination of various algorithms for items such as key exchange, authentication
 * type, bulk encryption algorithm, and message authentication code.
 *
 * @note Java users: See [[CipherSuitesConfig]].
 */
sealed trait CipherSuites

object CipherSuites {

  /**
   * Indicates that the determination for which cipher suites to use with the
   * particular engine should be delegated to the engine factory.
   */
  case object Unspecified extends CipherSuites

  /**
   * Indicates the cipher suites which should be enabled for a particular engine.
   *
   * @param ciphers A list of cipher suites listed in the order in which they
   * should be attempted to be used by the engine. The set of suites in this list
   * must be a subset of the set of suites supported by the underlying engine.
   *
   * {{{
   *   val suites = Seq(
   *     "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384",
   *     "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384")
   *   val cipherSuites = CipherSuites.Enabled(suites)
   * }}}
   */
  case class Enabled(ciphers: Seq[String]) extends CipherSuites

  /**
   * Converts a string list of cipher suites, separated by colons
   * to a [[CipherSuites]] value.
   */
  def fromString(ciphers: String): CipherSuites = {
    val suites = ciphers.split(":").toSeq.filterNot(_.isEmpty)
    if (suites.isEmpty) Unspecified
    else Enabled(suites)
  }
}
