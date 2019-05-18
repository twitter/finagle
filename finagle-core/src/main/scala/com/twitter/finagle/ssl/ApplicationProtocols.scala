package com.twitter.finagle.ssl

/**
 * ApplicationProtocols represent the prioritized list of Application-Layer Protocol
 * Negotiation (ALPN) or Next Protocol Negotiation (NPN) values that a configured TLS
 * [[Engine]] should support.
 *
 * @note Currently supported values include IANA Registered Application-Layer Protocol
 * Negotiation (ALPN) IDs and "spdy/3.1" which is commonly used with Next Protocol
 * Negotiation (NPN).
 * @note Java users: See [[ApplicationProtocolsConfig]].
 */
sealed trait ApplicationProtocols

object ApplicationProtocols {

  // IANA Application-Layer Protocol Negotiation (ALPN) IDs
  // From: https://www.iana.org/assignments/tls-extensiontype-values/tls-extensiontype-values.xhtml#alpn-protocol-ids
  private val alpnProtocolIds: Set[String] = Set(
    "http/1.1",
    "spdy/1",
    "spdy/2",
    "spdy/3",
    "stun.turn",
    "stun.nat-discovery",
    "h2",
    "h2c",
    "webrtc",
    "c-webrtc",
    "ftp"
  )

  private val otherProtocolIds: Set[String] = Set("spdy/3.1")

  private val combinedProtocolIds: Set[String] = alpnProtocolIds ++ otherProtocolIds

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
  case class Supported(appProtocols: Seq[String]) extends ApplicationProtocols {
    require(
      appProtocols.forall(combinedProtocolIds.contains),
      "Each value must be one of the following protocols: " +
        combinedProtocolIds.toSeq.sorted.mkString(",")
    )
  }

  /**
   * Converts a string list of application protocols, separated by commas
   * to an [[ApplicationProtocols]] value.
   *
   * @note Whitespace is allowed between values.
   */
  def fromString(appProtocols: String): ApplicationProtocols = {
    val appProtos = appProtocols
      .split(",")
      .view
      .map(_.trim)
      .filterNot(_.isEmpty)
      .toSeq
    if (appProtos.isEmpty) Unspecified
    else Supported(appProtos)
  }

  /**
   * Combines two [[ApplicationProtocols]] into one. This is used internally within
   * Finagle to combine [[ApplicationProtocols]] that are necessary for a communication
   * protocol to work and where a user may have declared a sequence of [[ApplicationProtocols]]
   * themselves.
   *
   * @note If both values are [[ApplicationProtocols.Supported]], the combination will consist
   * of the first group in its entirety, with the second group appended to the end minus duplicates
   * contained within the first group.
   */
  private[finagle] def combine(
    appProtocols1: ApplicationProtocols,
    appProtocols2: ApplicationProtocols
  ): ApplicationProtocols = {
    (appProtocols1, appProtocols2) match {
      case (_, ApplicationProtocols.Unspecified) => appProtocols1
      case (ApplicationProtocols.Unspecified, _) => appProtocols2
      case (ApplicationProtocols.Supported(protos1), ApplicationProtocols.Supported(protos2)) =>
        ApplicationProtocols.Supported((protos1 ++ protos2).distinct)
    }
  }
}
