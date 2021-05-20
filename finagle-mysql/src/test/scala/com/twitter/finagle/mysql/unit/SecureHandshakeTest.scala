package com.twitter.finagle.mysql.unit

import com.twitter.concurrent.AsyncQueue
import com.twitter.finagle.Stack
import com.twitter.finagle.mysql.transport.{MysqlTransport, Packet}
import com.twitter.finagle.mysql.{
  Capability,
  HandshakeInit,
  HandshakeStackModifier,
  SecureHandshake
}
import com.twitter.finagle.ssl.client.SslClientConfiguration
import com.twitter.finagle.ssl.{Protocols, TrustCredentials}
import com.twitter.finagle.transport.{QueueTransport, Transport}
import com.twitter.io.Buf
import com.twitter.util.Promise
import org.scalatest.funsuite.AnyFunSuite

class SecureHandshakeTest extends AnyFunSuite {
  val mysql5HandshakeInit = HandshakeInit(
    protocol = 121,
    version = "5.7.24",
    threadId = 0,
    salt = Array.fill[Byte](8)(0),
    serverCapabilities = Capability(0),
    charset = 8,
    status = 2)

  val startSslConfig = SslClientConfiguration(
    trustCredentials = TrustCredentials.Insecure,
    protocols = Protocols.Enabled(Seq("TLSv1.1")))
  val modifiedSslConfig = startSslConfig.copy(protocols = Protocols.Enabled(Seq("TLSv1.2")))

  val qtrans = new QueueTransport(new AsyncQueue[Buf](), new AsyncQueue[Buf]())
  val trans = new MysqlTransport(qtrans.map(_.toBuf, Packet.fromBuf))

  def modifyParams(params: Stack.Params, handshakeInit: HandshakeInit): Stack.Params = {
    val selectedConfig =
      if (handshakeInit.version.startsWith("5.")) startSslConfig else modifiedSslConfig
    params + Transport.ClientSsl(Some(selectedConfig))
  }

  val params =
    Stack.Params.empty + Transport.ClientSsl(Some(startSslConfig)) + HandshakeStackModifier(
      modifyParams)

  val handshake = new SecureHandshake(params, trans)

  test("getTlsParams selects Mysql5 stack params based on the handshake stack modifier") {
    val result = handshake.getTlsParams(mysql5HandshakeInit, new Promise[Unit])
    assert(result[Transport.ClientSsl].sslClientConfiguration.get == startSslConfig)
  }

  test("getTlsParams selects Mysql8 stack params based on the handshake stack modifier") {
    val result =
      handshake.getTlsParams(mysql5HandshakeInit.copy(version = "8.0.21"), new Promise[Unit])
    assert(result[Transport.ClientSsl].sslClientConfiguration.get == modifiedSslConfig)
  }
}
