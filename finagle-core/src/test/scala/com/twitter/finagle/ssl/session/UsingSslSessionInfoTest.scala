package com.twitter.finagle.ssl.session

import com.twitter.util.security.NullSslSession
import java.math.BigInteger
import java.security.cert.{Certificate, X509Certificate}
import javax.net.ssl.SSLSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

class UsingSslSessionInfoTest extends AnyFunSuite with MockitoSugar {

  val sessionID: Array[Byte] = (1 to 32).toArray.map(_.toByte)
  val localCerts: Array[Certificate] = Array.empty

  val localSerialNumber: BigInteger = new BigInteger("1")
  val peerSerialNumber: BigInteger = new BigInteger("2")

  val localCert: X509Certificate = mock[X509Certificate]
  when(localCert.getSerialNumber).thenReturn(localSerialNumber)

  val peerCert: X509Certificate = mock[X509Certificate]
  when(peerCert.getSerialNumber).thenReturn(peerSerialNumber)

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
    assert(sessionInfo.localCertificates.head.getSerialNumber == localSerialNumber)
  }

  test("UsingSslSessionInfo works when local certificates are null") {
    val nullLocalSession: SSLSession = mock[SSLSession]
    when(nullLocalSession.getId).thenReturn(sessionID)
    when(nullLocalSession.getLocalCertificates).thenReturn(null)
    when(nullLocalSession.getPeerCertificates).thenReturn(Array[Certificate](peerCert))
    when(nullLocalSession.getCipherSuite).thenReturn("my-made-up-cipher")

    val sessionInfo: SslSessionInfo = new UsingSslSessionInfo(nullLocalSession)
    assert(sessionInfo.localCertificates.length == 0)
  }

  test("UsingSslSessionInfo returns a peer X509Certificate") {
    assert(sessionInfo.peerCertificates.length == 1)
    assert(sessionInfo.peerCertificates.head.getSerialNumber == peerSerialNumber)
  }

}
