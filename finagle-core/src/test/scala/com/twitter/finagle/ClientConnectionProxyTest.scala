package com.twitter.finagle

import com.twitter.finagle.ssl.session.SslSessionInfo
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

class ClientConnectionProxyTest extends AnyFunSuite with MockitoSugar {

  test("ClientConnectionProxy uses underlying SSL/TLS session info") {
    val sslSessionInfo: SslSessionInfo = mock[SslSessionInfo]
    val underlying: ClientConnection = mock[ClientConnection]
    when(underlying.sslSessionInfo).thenReturn(sslSessionInfo)
    val conn: ClientConnection = new ClientConnectionProxy(underlying)
    assert(conn.sslSessionInfo == sslSessionInfo)
  }

}
