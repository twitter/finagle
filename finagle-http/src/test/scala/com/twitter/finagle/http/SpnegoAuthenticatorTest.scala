package com.twitter.finagle.http

import com.twitter.finagle.Service
import com.twitter.util.{Await, Future, Time}
import java.net.InetSocketAddress
import java.util.Arrays.{equals => arrayEquals}
import org.ietf.jgss.GSSContext
import org.junit.runner.RunWith
import org.mockito.Matchers.any
import org.mockito.Mockito.{stub, verify}
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class SpnegoAuthenticatorTest extends FunSuite with MockitoSugar {
  import SpnegoAuthenticator._

  def builder = RequestBuilder().url("http://0.0.0.0/arbitrary")
  def anyAuthenticated = any[Authenticated[Request]]

  test("no header") {
    negative(builder.buildGet())
  }

  test("bad header") {
    negative {
      builder.setHeader(Fields.Authorization, "foobar").buildGet()
    }
  }

  test("malformed token") {
    // TODO: c.t.u.Base64StringEncoder is crazy permissive: the only way to win is not to play
    negative {
      builder.setHeader(Fields.Authorization, AuthScheme).buildGet()
    }
  }

  test("success") {
    val credentials = mock[GSSContext]
    val clientToken: Token = Array[Byte](1,3,3,7)
    val credSrc = new Credentials.ClientSource with Credentials.ServerSource {
      def load() = Future(credentials)
      def init(c: GSSContext, t: Option[Token]) = Future(clientToken)
      def accept(c: GSSContext, t: Token) = {
        assert(arrayEquals(clientToken, t))
        Future(Negotiated(Some(c), Some("sure thing boss")))
      }
    }

    // Spnego-filtered client/server
    val (client, server, service) = serve(credSrc, Some(credSrc))
    val req = builder.buildGet()
    stub(service.apply(anyAuthenticated)).toReturn(
      Future(Response(req.version, Status.Ok))
    )
    try {
      // should succeed with exactly one authenticated request
      val resp = Await.result(client(req))
      assert(resp.status == Status.Ok)
      verify(service).apply(anyAuthenticated)
    } finally {
      server.close(Time.Bottom)
    }
  }

  /**
   * An unauthorized request to the server (no ClientFilter in place.)
   */
  def negative(req: Request): Response = {
    // negative tests will not reach the credential source
    val serverSrc = new Credentials.JAASServerSource("test-authenticated-service")
    val (client, server, _) = serve(serverSrc)
    try {
      val rsp = Await.result(client.apply(req))
      assert(rsp.status == Status.Unauthorized)
      rsp
    } finally {
      client.close().before(server.close())
    }
  }

  def serve(
    serverSrc: Credentials.ServerSource,
    clientSrc: Option[Credentials.ClientSource] = None
  ) = {
    val service = mock[Service[Authenticated[Request], Response]]
    val server = com.twitter.finagle.Http.serve(
      "localhost:*", new ServerFilter(serverSrc) andThen service)
    val port = server.boundAddress.asInstanceOf[InetSocketAddress].getPort
    val rawClient = com.twitter.finagle.Http.newService(s"localhost:$port")

    val client =
      clientSrc.map { src =>
        new ClientFilter(src) andThen rawClient
      }.getOrElse {
        rawClient
      }
    (client, server, service)
  }
}
