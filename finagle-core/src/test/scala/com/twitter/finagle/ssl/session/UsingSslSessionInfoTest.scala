package com.twitter.finagle.ssl.session

import com.twitter.finagle.ssl.session.ServiceIdentity.UriServiceIdentity
import com.twitter.io.TempFile
import com.twitter.util.security.NullSslSession
import com.twitter.util.security.X509CertificateFile
import java.security.cert.Certificate
import java.security.cert.X509Certificate
import javax.net.ssl.SSLSession
import org.mockito.Mockito.when
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar

class UsingSslSessionInfoTest extends AnyFunSuite with MockitoSugar {

  val sessionID: Array[Byte] = (1 to 32).toArray.map(_.toByte)

  private val localCertFile = TempFile.fromResourcePath("/ssl/certs/test-ec-with-sans.crt")
  private val peerCertFile = TempFile.fromResourcePath("/ssl/certs/test-ecclient-with-sans.crt")

  val localCert: X509Certificate =
    new X509CertificateFile(localCertFile).readX509Certificate().get()
  val peerCert: X509Certificate = new X509CertificateFile(peerCertFile).readX509Certificate().get()

  val mockSslSession: SSLSession = mock[SSLSession]
  when(mockSslSession.getId).thenReturn(sessionID)
  when(mockSslSession.getLocalCertificates).thenReturn(Array[Certificate](localCert))
  when(mockSslSession.getPeerCertificates).thenReturn(Array[Certificate](peerCert))
  when(mockSslSession.getCipherSuite).thenReturn("my-made-up-cipher")

  val sessionInfo: SslSessionInfo = new UsingSslSessionInfo(mockSslSession)

  test("UsingSslSessionInfo does not allow a NullSslSession") {
    intercept[IllegalArgumentException] {
      new UsingSslSessionInfo(NullSslSession)
    }
  }

  test("UsingSslSessionInfo is an SslSessionInfo") {
    assert(sessionInfo != null)
  }

  test("UsingSslSessionInfo does use SSL/TLS") {
    assert(sessionInfo.usingSsl)
  }

  test("UsingSslSessionInfo has an SSLSession") {
    assert(sessionInfo.session == mockSslSession)
  }

  test("UsingSslSessionInfo has a Session ID") {
    assert(sessionInfo.sessionId.length == 64)
    assert(
      sessionInfo.sessionId ==
        "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20")
  }

  test("UsingSslSessionInfo does have a cipher suite") {
    assert(sessionInfo.cipherSuite == "my-made-up-cipher")
  }

  test("UsingSslSessionInfo returns a local X509Certificate") {
    assert(sessionInfo.localCertificates.length == 1)
    assert(sessionInfo.localCertificates.head.getSerialNumber == localCert.getSerialNumber)
  }

  test("UsingSslSessionInfo works when local certificates are null") {
    val nullLocalSession: SSLSession = mock[SSLSession]
    when(nullLocalSession.getId).thenReturn(sessionID)
    when(nullLocalSession.getLocalCertificates).thenReturn(null)
    when(nullLocalSession.getPeerCertificates).thenReturn(Array[Certificate](peerCert))
    when(nullLocalSession.getCipherSuite).thenReturn("my-made-up-cipher")

    val sessionInfo: SslSessionInfo = new UsingSslSessionInfo(nullLocalSession)
    assert(sessionInfo.localCertificates.isEmpty)
  }

  test("UsingSslSessionInfo returns a peer X509Certificate") {
    assert(sessionInfo.peerCertificates.length == 1)
    assert(sessionInfo.peerCertificates.head.getSerialNumber == peerCert.getSerialNumber)
  }

  test("UsingSslSessionInfo returns local and peer identities") {
    assert(sessionInfo.localIdentity.get.isInstanceOf[UriServiceIdentity])
    assert(sessionInfo.peerIdentity.get.isInstanceOf[UriServiceIdentity])
    assert(sessionInfo.localIdentity.get.name == "twtr:svc:csl-test:test-ecserver:devel:local")
    assert(sessionInfo.peerIdentity.get.name == "twtr:svc:csl-test:test-ecclient:devel:local")
  }

}
