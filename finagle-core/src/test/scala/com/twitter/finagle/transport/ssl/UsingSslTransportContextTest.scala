package com.twitter.finagle.transport.ssl

import com.twitter.util.security.NullSslSession
import java.math.BigInteger
import java.security.cert.{Certificate, X509Certificate}
import javax.net.ssl.SSLSession
import org.mockito.Mockito.when
import org.scalatest.FunSuite
import org.scalatest.mockito.MockitoSugar

class UsingSslTransportContextTest extends FunSuite with MockitoSugar {

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

  val context: SslTransportContext = new UsingSslTransportContext(mockSslSession)

  test("UsingSslTransportContext does not allow a NullSslSession") {
    intercept[IllegalArgumentException] {
      new UsingSslTransportContext(NullSslSession)
    }
  }

  test("UsingSslTransportContext is an SslTransportContext") {
    assert(context != null)
  }

  test("UsingSslTransportContext does use SSL/TLS") {
    assert(context.usingSsl)
  }

  test("UsingSslTransportContext has an SSLSession") {
    assert(context.session == mockSslSession)
  }

  test("UsingSslTransportContext has a Session ID") {
    assert(context.sessionId.length == 64)
    assert(context.sessionId == "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20")
  }

  test("UsingSslTransportContext does have a cipher suite") {
    assert(context.cipherSuite == "my-made-up-cipher")
  }

  test("UsingSslTransportContext returns a local X509Certificate") {
    assert(context.localCertificates.length == 1)
    assert(context.localCertificates.head.getSerialNumber == localSerialNumber)
  }

  test("UsingSslTransportContext returns a peer X509Certificate") {
    assert(context.peerCertificates.length == 1)
    assert(context.peerCertificates.head.getSerialNumber == peerSerialNumber)
  }

}
