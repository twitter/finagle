package com.twitter.finagle.transport.ssl

import com.twitter.util.security.NullSslSession
import org.scalatest.FunSuite

class NoSslTransportContextTest extends FunSuite {

  test("NoSslTransportContext is an SslTransportContext") {
    val context: SslTransportContext = NoSslTransportContext
  }

  test("NoSslTransportContext does not use SSL/TLS") {
    assert(!NoSslTransportContext.usingSsl)
  }

  test("NoSslTransportContext does not have an SSLSession") {
    assert(NoSslTransportContext.session == NullSslSession)
  }

  test("NoSslTransportContext does not have a Session ID") {
    assert(NoSslTransportContext.sessionId == "")
  }

  test("NoSslTransportContext does not have a cipher suite") {
    assert(NoSslTransportContext.cipherSuite == "")
  }

  test("NoSslTransportContext does not have certificates") {
    assert(NoSslTransportContext.localCertificates.length == 0)
    assert(NoSslTransportContext.peerCertificates.length == 0)
  }

}
