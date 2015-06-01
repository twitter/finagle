package com.twitter.finagle.http

import com.twitter.finagle.http.netty.HttpRequestProxy
import com.twitter.finagle.{Service, Filter}
import com.twitter.util.{Base64StringEncoder, Future, FuturePool, Return}
import com.twitter.logging.Logger
import java.security.PrivilegedAction
import javax.security.auth.Subject
import javax.security.auth.login.LoginContext
import org.ietf.jgss._
import org.jboss.netty.channel.Channel
import org.jboss.netty.handler.codec.http.{
  HttpRequest,
  HttpResponse,
  HttpHeaders,
  HttpVersion,
  DefaultHttpResponse
}
import scala.collection.JavaConverters._

/**
 * A SPNEGO HTTP authenticator as defined in https://tools.ietf.org/html/rfc4559, which gets
 * its credentials from a provided CredentialSource... usually JAAS.
 */
object SpnegoAuthenticator {
  private val log = Logger("spnego")

  type Token = Array[Byte]
  object Token {
    val Empty = Array.empty[Byte]
  }

  val AuthScheme = "Negotiate"

  private object AuthHeader {
    val SchemePrefixLength = AuthScheme.length + 1
    def apply(token: Option[Token]): Option[String] =
      token map { t =>
        AuthScheme + " " + Base64StringEncoder.encode(t)
      }

    /** If the header represents a valid spnego negotiation, return it. */
    def unapply(header: String): Option[Token] =
      // must be a valid Negotiate header, and have a token
      if (header.length <= SchemePrefixLength || !header.startsWith(AuthScheme)) {
        None
      } else {
        val tokenStr = header.substring(SchemePrefixLength)
        Some(Base64StringEncoder.decode(tokenStr))
      }
  }

  /** An authenticated HTTP request, with its GSS context. */
  trait Authenticated[Req] {
    val request: Req
    val context: GSSContext
  }

  object Authenticated {
    case class Http(httpRequest: HttpRequest, context: GSSContext)
         extends HttpRequestProxy
         with Authenticated[HttpRequest] {
      val request = httpRequest
    }

    case class RichHttp(request: Request, context: GSSContext)
         extends RequestProxy
         with Authenticated[Request]
  }

  case class Negotiated(
    established: Option[GSSContext],
    wwwAuthenticate: Option[String])

  object Credentials {
    trait ServerSource {
      /** Loads a GSSContext (for previously specified identifiers).  */
      def load(): Future[GSSContext]
      /**
       * Called by a server to decide whether to accept the given token to create a context.
       * Returns a Negotitated representing the response.
       */
      def accept(context: GSSContext, negotiation: Token): Future[Negotiated]
    }

    trait ClientSource {
      /** Loads a GSSContext (for previously specified identifiers).  */
      def load(): Future[GSSContext]
      /**
       * Called by a client to initialize the security context and return the next Token
       * to send to the server. ChallengeToken may be empty if we haven't been challenged.
       */
      def init(context: GSSContext, challengeToken: Option[Token]): Future[Token]
    }

    /**
     * JAAS implementation of credential fetching/validation (via the configuration section named by
     * loginContext).  If a principal or OID is provided, it is used to override the settings in the JAAS
     * configuration file.
     *
     * Since the authentication operations may block when i.e. talking to a KDC, these potentially
     * blocking calls are wrapped in a FuturePool.
     */
    object JAAS {
      /**
       * Oid for the KRB5 mechanism  These come from
       * http://www.oid-info.com/get/1.2.840.113554.1.2.2
       *
       */
      val Krb5Mechanism = new Oid("1.2.840.113554.1.2.2")
      val Krb5PrincipalType = new Oid("1.2.840.113554.1.2.2.1")
      /**
       * Oid for the Spnego mechanism  These come from
       * http://www.oid-info.com/get/1.3.6.1.5.5.2
       *
       */
      val SpnegoMechanism = new Oid("1.3.6.1.5.5.2")
    }

    trait JAAS {
      val loginContext: String

      def load(): Future[GSSContext] = pool {
        log.debug("Getting context: %s", loginContext)
        val portal = new LoginContext(loginContext)
        // TODO: should logout?
        portal.login()
        Subject.doAs(portal.getSubject, createContextAction)
      }

      private val createContextAction =
        new PrivilegedAction[GSSContext] {
          def run(): GSSContext = createGSSContext()
        }

      /** Called while running with the privileges of the given loginContext.  */
      protected def createGSSContext(): GSSContext

      /** A processes' own principal is usually specified via {{sun.security.krb5.principal}}. */
      protected def selfPrincipal: Option[GSSName] = None

      protected def lifetime: Int = GSSContext.DEFAULT_LIFETIME
      protected def mechanism: Oid = JAAS.Krb5Mechanism
      protected def manager: GSSManager = GSSManager.getInstance
      protected def pool: FuturePool = FuturePool.unboundedPool
    }

    class JAASClientSource(
      val loginContext: String,
      _serverPrincipal: String,
      _serverPrincipalType: Oid = JAAS.Krb5PrincipalType
    ) extends ClientSource with JAAS {
      val serverPrincipal = manager.createName(_serverPrincipal, _serverPrincipalType)

      def init(context: GSSContext, challengeToken: Option[Token]): Future[Token] = pool {
        val tokenIn = challengeToken.getOrElse(Token.Empty)
        var tokenOut: Token = null
        do {
          tokenOut = context.initSecContext(tokenIn, 0, tokenIn.length)
        } while (tokenOut == null);
        tokenOut
      }

      protected def createGSSContext(): GSSContext =
        manager.createContext(
          serverPrincipal,
          mechanism,
          manager.createCredential(
            selfPrincipal.orNull,
            lifetime,
            mechanism,
            GSSCredential.INITIATE_ONLY
          ),
          lifetime
        )
    }

    class JAASServerSource(val loginContext: String) extends ServerSource with JAAS {
      def accept(context: GSSContext, negotiation: Token): Future[Negotiated] = pool {
        val token = context.acceptSecContext(negotiation, 0, negotiation.length)
        val established = if (context.isEstablished) Some(context) else None
        val wwwAuthenticate = AuthHeader(Option(token))
        Negotiated(established, wwwAuthenticate)
      }

      protected def createGSSContext(): GSSContext = {
        val cred = manager.createCredential(
          selfPrincipal.orNull,
          lifetime,
          JAAS.SpnegoMechanism,
          GSSCredential.ACCEPT_ONLY
        )
        cred.add(
          selfPrincipal.orNull,
          lifetime,
          lifetime,
          JAAS.Krb5Mechanism,
          GSSCredential.ACCEPT_ONLY
        )
        manager.createContext(cred)
      }
    }
  }

  /** A typeclass to get/set fields of http responses of type Rsp. */
  sealed trait RspSupport[Rsp] {
    /** Get the status for the Rsp. */
    def status(rsp: Rsp): Status.Type
    /** Get the WWW-Authenticate: Negotiate <token> header. */
    def wwwAuthenticateHeader(rsp: Rsp): Option[String]
    /** Sets the WWW-Authenticate: Negotiate <token> header. */
    def wwwAuthenticateHeader(rsp: Rsp, auth: String): Unit
    /** Create an Unauthorized response with the given protocolVersion. */
    def unauthorized(version: HttpVersion): Rsp
  }

  /** A typeclass to get/set fields of http requests of type Req. */
  sealed trait ReqSupport[Req] {
    /** Returns the AUTHORIZATION header for the given Req. */
    def authorizationHeader(req: Req): Option[String]
    /** Sets the AUTHORIZATION header for the given Req. */
    def authorizationHeader(req: Req, token: Token): Unit
    /** Get the protocol version for a request. */
    def protocolVersion(req: Req): HttpVersion
    /** Wrap a Req with authentication information. */
    def authenticated(req: Req, context: GSSContext): Authenticated[Req]
  }

  implicit val httpResponseSupport = new RspSupport[HttpResponse] {
    def status(rsp: HttpResponse) = rsp.getStatus
    def wwwAuthenticateHeader(rsp: HttpResponse) =
      Option(rsp.headers.get(HttpHeaders.Names.WWW_AUTHENTICATE))
    def wwwAuthenticateHeader(rsp: HttpResponse, auth: String) =
      rsp.headers.set(HttpHeaders.Names.WWW_AUTHENTICATE, auth)
    def unauthorized(version: HttpVersion) = new DefaultHttpResponse(version, Status.Unauthorized)
  }

  implicit val httpRequestSupport = new ReqSupport[HttpRequest] {
    def authorizationHeader(req: HttpRequest) =
      Option(req.headers.get(HttpHeaders.Names.AUTHORIZATION))
    def authorizationHeader(req: HttpRequest, token: Token) =
      AuthHeader(Some(token)).foreach { header =>
        req.headers.set(HttpHeaders.Names.AUTHORIZATION, header)
      }
    def protocolVersion(req: HttpRequest) = req.getProtocolVersion
    def authenticated(req: HttpRequest, context: GSSContext) =
      Authenticated.Http(req, context)
  }

  implicit val httpRichResponseSupport = new RspSupport[Response] {
    def status(rsp: Response) = rsp.status
    def wwwAuthenticateHeader(rsp: Response) = rsp.authorization
    def wwwAuthenticateHeader(rsp: Response, auth: String) = { rsp.wwwAuthenticate = auth }
    def unauthorized(version: HttpVersion) = Response(version, Status.Unauthorized)
  }

  implicit val httpRichRequestSupport = new ReqSupport[Request] {
    def authorizationHeader(req: Request) = req.authorization
    def authorizationHeader(req: Request, token: Token) =
      AuthHeader(Some(token)).foreach { header => req.authorization = header }
    def protocolVersion(req: Request) = req.version
    def authenticated(req: Request, context: GSSContext) = Authenticated.RichHttp(req, context)
  }

  sealed abstract class Client[Req: ReqSupport, Rsp: RspSupport]
      extends Filter[Req, Rsp, Req, Rsp] {
    val credSrc: Credentials.ClientSource
    val reqs = implicitly[ReqSupport[Req]]
    val rsps = implicitly[RspSupport[Rsp]]

    /**
     * TODO: naive implementation: every request arriving at the filter begins from the
     * beginning of the protocol.
     */
    def apply(req: Req, backend: Service[Req, Rsp]): Future[Rsp] =
      challengeResponseLoop(req, backend, None)

    /** Repeats the challenge/response loop until GSS Tokens are accepted by the server. */
    private def challengeResponseLoop(
      req: Req,
      backend: Service[Req,Rsp],
      credentialOption: Option[Future[GSSContext]]
    ): Future[Rsp] =
      backend(req).transform {
        case Return(rsp) if rsps.status(rsp) == Status.Unauthorized =>
          // we've been challenged: reattempt the request with an improved token
          val credentialFuture = credentialOption.getOrElse(credSrc.load())
          credentialFuture.flatMap { context =>
            // look for any Token data in the challenge, and attempt to initialize
            val challengeToken =
              rsps.wwwAuthenticateHeader(rsp).collect {
                case AuthHeader(token) => token
              }
            credSrc.init(context, challengeToken).flatMap { nextToken =>
              // loop to reattempt the request, mutated with the next token data
              reqs.authorizationHeader(req, nextToken)
              challengeResponseLoop(req, backend, Some(credentialFuture))
            }
          }
        case rsp =>
          // no challenge (or an exception): we're finished
          Future.const(rsp)
      }
  }

  sealed abstract class Server[Req: ReqSupport, Rsp: RspSupport]
      extends Filter[Req, Rsp, SpnegoAuthenticator.Authenticated[Req], Rsp] {
    val credSrc: Credentials.ServerSource
    val reqs = implicitly[ReqSupport[Req]]
    val rsps = implicitly[RspSupport[Rsp]]

    private def unauthorized(req: Req) = {
      val rsp = rsps.unauthorized(reqs.protocolVersion(req))
      rsps.wwwAuthenticateHeader(rsp, AuthScheme)
      rsp
    }

    final def apply(req: Req, authed: Service[Authenticated[Req], Rsp]): Future[Rsp] =
      reqs.authorizationHeader(req).collect {
        case AuthHeader(negotiation) =>
          credSrc.load() flatMap {
            credSrc.accept(_, negotiation)
          } flatMap { negotiated =>
            negotiated.established map { ctx =>
              authed(reqs.authenticated(req, ctx))
            } getOrElse {
              Future value unauthorized(req)
            } map { rsp =>
              negotiated.wwwAuthenticate foreach {
                rsps.wwwAuthenticateHeader(rsp, _)
              }
              rsp
            }
          } handle {
            case e: GSSException => {
              log.error(e, "authenticating")
              unauthorized(req)
            }
          }
      } getOrElse {
        log.debug("Request had no AuthHeader information.  Returning Unauthorized.")
        Future value unauthorized(req)
      }
  }

  case class ClientFilter(credSrc: Credentials.ClientSource)
      extends Client[HttpRequest,HttpResponse]

  case class RichClientFilter(credSrc: Credentials.ClientSource)
      extends Client[Request,Response]

  case class ServerFilter(credSrc: Credentials.ServerSource)
      extends Server[HttpRequest,HttpResponse]

  case class RichServerFilter(credSrc: Credentials.ServerSource)
      extends Server[Request,Response]
}
