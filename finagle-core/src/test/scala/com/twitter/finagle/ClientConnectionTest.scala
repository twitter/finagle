package com.twitter.finagle

import com.twitter.finagle.ssl.session.NullSslSessionInfo
import org.scalatest.funsuite.AnyFunSuite

class ClientConnectionTest extends AnyFunSuite {

  test("ClientConnection.nil has NullSslSessionInfo") {
    val conn: ClientConnection = ClientConnection.nil
    assert(conn.sslSessionInfo == NullSslSessionInfo)
  }

}
