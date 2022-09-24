package com.twitter.finagle.ssl.session

import java.security.cert.X509Certificate
import javax.net.ssl.SSLSession

/**
 * `SslSessionInfo` provides information related to an existing connection's use
 * of an SSL/TLS session. SSL/TLS sessions are either negotiated or resumed
 * during handshaking. When using SSL/TLS, one and only one SSL/TLS session is
 * associated with a connection, although it's possible that an SSL/TLS session
 * may be reused between multiple connections.
 *
 * An instance of this class should be associated with an existing connection.
 * When using SSL/TLS, the instance provides easy access to the session and its
 * relevant information. When not using SSL/TLS (i.e. there is no existing
 * SSL/TLS session), then a `NullSslSessionInfo` should be used instead.
 */
abstract class SslSessionInfo {

  /**
   * Indicates whether the connection is using SSL/TLS.
   */
  def usingSsl: Boolean

  /**
   * The `SSLSession` associated with a particular connection.
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

  lazy val localIdentity: Option[ServiceIdentity] = getLocalIdentity
  lazy val peerIdentity: Option[ServiceIdentity] = getPeerIdentity

  protected def getLocalIdentity: Option[ServiceIdentity]
  protected def getPeerIdentity: Option[ServiceIdentity]

}
