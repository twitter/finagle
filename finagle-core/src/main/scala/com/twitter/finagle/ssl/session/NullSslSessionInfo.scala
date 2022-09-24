package com.twitter.finagle.ssl.session

import com.twitter.util.security.NullSslSession
import java.security.cert.X509Certificate
import javax.net.ssl.SSLSession

/**
 * Null object which indicates that the existing connection
 * is not using SSL/TLS.
 */
object NullSslSessionInfo extends SslSessionInfo {

  /**
   * Indicates whether the connection is using SSL/TLS.
   *
   * @return The returned value is always false.
   */
  def usingSsl: Boolean = false

  /**
   * The `SSLSession` associated with a particular connection.
   * Indicates that there is no `SSLSession` associated with this connection.
   *
   * @return The returned value is always a `NullSslSession`.
   */
  def session: SSLSession = NullSslSession

  /**
   * The Session ID associated with an `SSLSession`.
   *
   * @return The returned value is always an empty string.
   */
  def sessionId: String = ""

  /**
   * The cipher suite associated with an `SSLSession`.
   *
   * @return The returned value is always an empty string.
   */
  def cipherSuite: String = ""

  /**
   * The `X509Certificate`s that were sent to the peer during the SSL/TLS handshake.
   *
   * @return The returned value is always an empty sequence.
   */
  def localCertificates: Seq[X509Certificate] = Nil

  /**
   * The `X509Certificate`s that were received from the peer during the SSL/TLS handshake.
   *
   * @return The returned value is always an empty sequence.
   */
  def peerCertificates: Seq[X509Certificate] = Nil

  override protected def getLocalIdentity: Option[ServiceIdentity] = None
  override protected def getPeerIdentity: Option[ServiceIdentity] = None
}
