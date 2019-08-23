package com.twitter.finagle.ssl

import java.net.Socket
import java.security.cert.X509Certificate
import javax.net.ssl.SSLEngine
import org.scalatest.FunSuite
import org.scalatestplus.mockito.MockitoSugar

class IgnorantTrustManagerTest extends FunSuite with MockitoSugar {

  val authType = "DHE_DSS"
  val socket = mock[Socket]
  val engine = mock[SSLEngine]
  val cert = mock[X509Certificate]
  val chain = Array(cert)

  test("an IgnorantTrustManager can be created") {
    val tm = new IgnorantTrustManager()
    assert(tm != null)
  }

  test("an IgnorantTrustManager has no accepted issuers") {
    val tm = new IgnorantTrustManager()
    val issuers = tm.getAcceptedIssuers()
    assert(issuers.length == 0)
  }

  test("checkClientTrusted does not throw") {
    val tm = new IgnorantTrustManager()
    tm.checkClientTrusted(chain, authType)
  }

  test("checkClientTrusted with socket does not throw") {
    val tm = new IgnorantTrustManager()
    tm.checkClientTrusted(chain, authType, socket)
  }

  test("checkClientTrusted with engine does not throw") {
    val tm = new IgnorantTrustManager()
    tm.checkClientTrusted(chain, authType, engine)
  }

  test("checkServerTrusted does not throw") {
    val tm = new IgnorantTrustManager()
    tm.checkServerTrusted(chain, authType)
  }

  test("checkServerTrusted with socket does not throw") {
    val tm = new IgnorantTrustManager()
    tm.checkServerTrusted(chain, authType, socket)
  }

  test("checkServerTrusted with engine does not throw") {
    val tm = new IgnorantTrustManager()
    tm.checkServerTrusted(chain, authType, engine)
  }

}
