package com.twitter.finagle

import com.twitter.finagle.ssl.session.NullSslSessionInfo
import org.scalatest.FunSuite

class ClientConnectionTest extends FunSuite {

  test("ClientConnection.nil has NullSslSessionInfo") {
    val conn: ClientConnection = ClientConnection.nil
    assert(conn.sslSessionInfo == NullSslSessionInfo)
  }

}
