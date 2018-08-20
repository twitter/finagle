package com.twitter.finagle.transport.ssl

import com.twitter.conversions.string._
import com.twitter.util.security.NullSslSession
import java.security.cert.X509Certificate
import javax.net.ssl.SSLSession

/**
 * Local context which indicates that SSL/TLS is being used
 * on the transport for a particular request.
 *
 * @param session The `SSLSession` associated with the transport.
 */
private[finagle] final class UsingSslTransportContext(val session: SSLSession)
  extends SslTransportContext {

  // This class should not be used with a `NullSslSession`. Use
  // `NoSslTransportContext` instead.
  require(session != NullSslSession)

  /**
   * Indicates whether the transport is using SSL/TLS.
   *
   * @return The returned value is always true.
   */
  def usingSsl: Boolean = true

  /**
   * The Session ID associated with an `SSLSession`.
   *
   * @note The maximum length for an SSL/TLS Session ID is 32 bytes. This method
   * returns a hex string version of the Session ID which has a maximum length of 64 bytes.
   *
   * @return a hex string version of the raw byte Session ID.
   */
  val sessionId: String = session.getId.hexlify

  /**
   * The cipher suite associated with an `SSLSession`.
   *
   * @return The name of the session's cipher suite.
   */
  def cipherSuite: String = session.getCipherSuite

  /**
   * The `X509Certificate`s that were sent to the peer during the SSL/TLS handshake.
   *
   * @note If certificates are `Certificate` values but not `X509Certificate` values,
   * they will not be returned via this field. Instead use the `SSLSession#getLocalCertificates`
   * method to retrieve those local certificates.
   *
   * @return The sequence of local certificates sent.
   */
  val localCertificates: Seq[X509Certificate] =
    session.getLocalCertificates.toSeq.collect { case cert: X509Certificate => cert }

  /**
   * The `X509Certificate`s that were received from the peer during the SSL/TLS handshake.
   *
   * @note If certificates are `Certificate` values but not `X509Certificate` values,
   * they will not be returned via this field. Instead use the `SSLSession#getPeerCertificates`
   * method to retrieve those peer certificates.
   *
   * @return The sequence of peer certificates received.
   */
  val peerCertificates: Seq[X509Certificate] =
    session.getPeerCertificates.toSeq.collect { case cert: X509Certificate => cert }

}
