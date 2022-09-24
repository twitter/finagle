package com.twitter.finagle.ssl.session

import com.twitter.conversions.StringOps._
import com.twitter.util.security.ProxyX509Certificate
import com.twitter.util.security.NullSslSession
import java.security.cert.X509Certificate
import javax.net.ssl.SSLSession
import org.bouncycastle.asn1.x509.Extension
import org.bouncycastle.asn1.x509.GeneralName
import org.bouncycastle.asn1.x509.GeneralNames
import org.bouncycastle.cert.X509CertificateHolder

/**
 * Class which indicates that a particular connection is using SSL/TLS,
 * and provides some shortcuts to relevant pieces of `SSLSession` data.
 *
 * @param session The `SSLSession` associated with the connection.
 */
private[finagle] class UsingSslSessionInfo(
  val session: SSLSession)
    extends SslSessionInfo {

  // This class should not be used with a `NullSslSession`. Use
  // `NullSslSessionInfo` instead.
  require(session != NullSslSession)

  /**
   * Indicates whether the connection is using SSL/TLS.
   *
   * @return The returned value is always true.
   */
  def usingSsl: Boolean = true

  /**
   * The Session ID associated with an `SSLSession`.
   *
   * @note The maximum length for an SSL/TLS Session ID is 32 bytes. This method
   *       returns a hex string version of the Session ID which has a maximum length of 64 bytes.
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
   *       they will not be returned via this field. Instead use the `SSLSession#getLocalCertificates`
   *       method to retrieve those local certificates.
   * @return The sequence of local certificates sent.
   */
  val localCertificates: Seq[X509Certificate] = {
    val localCerts = session.getLocalCertificates
    if (localCerts == null) Nil
    else
      localCerts.toSeq.collect {
        case cert: X509Certificate =>
          new SystemIdentityCertificate(cert)
      }
  }

  /**
   * Private helper to extract the general names from the SubjectAlternativeName x509 extension field of a cert.
   */
  private def getSans(certificate: X509Certificate): Seq[GeneralName] = {
    val holder = new X509CertificateHolder(certificate.getEncoded)
    Option(
      GeneralNames
        .fromExtensions(holder.getExtensions, Extension.subjectAlternativeName))
      .map(_.getNames.toSeq).getOrElse(Nil)
  }

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
    session.getPeerCertificates.toSeq.collect {
      case cert: X509Certificate =>
        new SystemIdentityCertificate(cert)
    }

  override def getLocalIdentity: Option[ServiceIdentity] =
    localCertificates.headOption.map(getSans).flatMap(ServiceIdentity.apply)
  override def getPeerIdentity: Option[ServiceIdentity] =
    peerCertificates.headOption.map(getSans).flatMap(ServiceIdentity.apply)
}

private[session] class SystemIdentityCertificate(underlying: X509Certificate)
    extends ProxyX509Certificate(underlying) {
  override def equals(other: Any): Boolean = {
    other match {
      case identityCert: SystemIdentityCertificate => this eq identityCert
      case _ => false
    }
  }

  override def hashCode: Int = System.identityHashCode(this)
}
