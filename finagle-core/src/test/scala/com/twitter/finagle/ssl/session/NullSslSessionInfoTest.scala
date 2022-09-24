package com.twitter.finagle.ssl.session

import com.twitter.util.security.NullSslSession
import org.scalatest.funsuite.AnyFunSuite

class NullSslSessionInfoTest extends AnyFunSuite {

  test("NullSslSessionInfo is an SslSessionInfo") {
    val sessionInfo: SslSessionInfo = NullSslSessionInfo
  }

  test("NullSslSessionInfo does not use SSL/TLS") {
    assert(!NullSslSessionInfo.usingSsl)
  }

  test("NullSslSessionInfo does not have an SSLSession") {
    assert(NullSslSessionInfo.session == NullSslSession)
  }

  test("NullSslSessionInfo does not have a Session ID") {
    assert(NullSslSessionInfo.sessionId == "")
  }

  test("NullSslSessionInfo does not have a cipher suite") {
    assert(NullSslSessionInfo.cipherSuite == "")
  }

  test("NullSslSessionInfo does not have certificates") {
    assert(NullSslSessionInfo.localCertificates.length == 0)
    assert(NullSslSessionInfo.peerCertificates.length == 0)
  }

  test("NullSslSessionInfo does not have identities") {
    assert(NullSslSessionInfo.localIdentity.isEmpty)
    assert(NullSslSessionInfo.peerIdentity.isEmpty)
  }

}
