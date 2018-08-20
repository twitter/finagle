package com.twitter.finagle.transport.ssl

import javax.net.ssl.SSLSession
import java.security.cert.X509Certificate

/**
 * Local context which provides information about the transport
 * security associated with a particular request.
 */
private[finagle] trait SslTransportContext {

  /**
   * Indicates whether the transport is using SSL/TLS.
   */
  def usingSsl: Boolean

  /**
   * The `SSLSession` associated with a particular request.
   *
   * @note If SSL/TLS is not being used a `NullSslSession` will be returned instead.
   */
  def session: SSLSession

  /**
   * The Session ID associated with an `SSLSession`.
   *
   * @note The maximum length for an SSL/TLS Session ID is 32 bytes. This method returns
   * a hex string version of the Session ID which has a maximum length of 64 bytes.
   *
   * @note If SSL/TLS is not being used, an empty string will be returned instead.
   */
  def sessionId: String

  /**
   * The cipher suite associated with an `SSLSession`.
   *
   * @note If SSL/TLS is not being used, an empty string will be returned instead.
   */
  def cipherSuite: String

  /**
   * The `X509Certificate`s that were sent to the peer during the SSL/TLS handshake.
   *
   * @note If SSL/TLS is not being used, an empty sequence will be returned instead.
   */
  def localCertificates: Seq[X509Certificate]

  /**
   * The `X509Certificate`s that were received from the peer during the SSL/TLS handshake.
   *
   * @note If SSL/TLS is not being used, an empty sequence will be returned instead.
   */
  def peerCertificates: Seq[X509Certificate]

}
