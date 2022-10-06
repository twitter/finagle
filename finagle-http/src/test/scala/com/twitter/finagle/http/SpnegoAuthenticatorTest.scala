package com.twitter.finagle.http

import com.twitter.finagle.Service
import com.twitter.util.Await
import com.twitter.util.Future
import com.twitter.util.Time
import java.net.InetAddress
import java.net.InetSocketAddress
import java.security.Principal
import java.security.PrivilegedAction
import java.util.Arrays.{equals => arrayEquals}
import javax.security.auth.Subject
import javax.security.auth.kerberos.KerberosPrincipal
import javax.security.auth.kerberos.KerberosTicket
import javax.security.auth.login.LoginContext
import org.ietf.jgss.GSSContext
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.mockito.Mockito.verify
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

class SpnegoAuthenticatorTest extends AnyFunSuite with MockitoSugar {
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
    // TODO: c.t.u.Base64StringEncoder is too permissive: the only way to win is not to play
    negative {
      builder.setHeader(Fields.Authorization, AuthScheme).buildGet()
    }
  }

  test("success") {
    val credentials = mock[GSSContext]
    val clientToken: Token = Array[Byte](1, 3, 3, 7)
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
    when(service.apply(anyAuthenticated)).thenReturn(
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

  test("isLoginValid returns correct result") {

    /** Temp class so we can expose isLoginValid which is protected by the JAAS trait */
    class JaasLoginTest extends Credentials.ClientSource with Credentials.JAAS {
      override def init(context: GSSContext, challengeToken: Option[Token]): Future[Token] = ???

      override val loginContext: String = ""

      override protected def createGSSContext(): GSSContext = ???

      def testIsLoginValid(portal: Option[LoginContext]) = super.isLoginValid(portal)

    }

    /** Subject credentials are secured so we need a privileged action to modify them */
    def addCreds(subject: Subject, ticket: KerberosTicket) =
      new PrivilegedAction[Boolean] {
        def run(): Boolean = subject.getPrivateCredentials(classOf[KerberosTicket]).add(ticket)
      }

    def createTicketWithEndTime(end: Time, principal: KerberosPrincipal): KerberosTicket =
      new KerberosTicket(
        new Array[Byte](10),
        principal,
        principal,
        new Array[Byte](10),
        0,
        new Array[Boolean](10),
        Time.now.toDate,
        Time.now.toDate,
        end.toDate,
        Time.Top.toDate,
        new Array[InetAddress](10)
      )

    def createSubjectWithTicket(ticket: KerberosTicket): Subject = {
      val creds = new java.util.HashSet[KerberosTicket]()
      creds.add(ticket)
      new Subject(
        false,
        new java.util.HashSet[Principal](),
        new java.util.HashSet(),
        creds
      )
    }

    val jaas = new JaasLoginTest
    // Needs to start with 'krbtgt/' and have a realm (@REALM.NAME).
    val principal = new KerberosPrincipal("krbtgt/random@TEST.REALM")

    // End date for TGT has not passed yet, ticket is valid
    val goodTicket = createTicketWithEndTime(Time.Top, principal)
    val goodSubject = createSubjectWithTicket(goodTicket)
    val goodLoginContext = mock[LoginContext]
    when(goodLoginContext.getSubject).thenReturn(goodSubject)
    assert(jaas.testIsLoginValid(Some(goodLoginContext)))

    // End date for TGT has already passed, ticket is invalid
    val badTicket = createTicketWithEndTime(Time.Bottom, principal)
    val badSubject = createSubjectWithTicket(badTicket)
    val badLoginContext = mock[LoginContext]
    when(badLoginContext.getSubject).thenReturn(badSubject)
    assert(!jaas.testIsLoginValid(Some(badLoginContext)))
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
    val server =
      com.twitter.finagle.Http.serve("localhost:*", new ServerFilter(serverSrc) andThen service)
    val port = server.boundAddress.asInstanceOf[InetSocketAddress].getPort
    val rawClient = com.twitter.finagle.Http.newService(s"localhost:$port")

    val client =
      clientSrc
        .map { src => new ClientFilter(src) andThen rawClient }
        .getOrElse {
          rawClient
        }
    (client, server, service)
  }
}
