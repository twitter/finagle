package com.twitter.finagle.http

import com.sun.security.jgss.GSSUtil
import com.twitter.finagle.http.netty.HttpRequestProxy
import com.twitter.finagle.{Service, Filter}
import com.twitter.util.{Base64StringEncoder, Future, FuturePool}
import com.twitter.logging.Logger
import java.security.PrivilegedAction
import javax.security.auth.Subject
import javax.security.auth.login.LoginContext
import org.ietf.jgss._
import org.jboss.netty.handler.codec.http.{
  HttpRequest,
  HttpResponse,
  HttpHeaders,
  DefaultHttpResponse
}
import scala.collection.JavaConverters._

object SpnegoAuthenticator {
  private val log = Logger("spnego")

  val AuthScheme = "Negotiate"

  object AuthHeader {
    def unapply(header: String): Option[Option[String]] = {
      if (header startsWith AuthScheme) Some {
        if (header.length > AuthScheme.length) Some {
          header.substring(AuthScheme.length + 1)
        } else None
      } else None
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

  class HttpFilter(
    loginContext: String,
    principal: Option[GSSName] = None,
    oid: Option[Oid] = None,
    manager: GSSManager = GSSManager.getInstance,
    pool: FuturePool = FuturePool.unboundedPool)
  extends SpnegoAuthenticator[HttpRequest, HttpResponse](
    loginContext, principal, oid, manager, pool) {

    protected[this] def getNegotiation(req: HttpRequest): Option[Array[Byte]] =
      Option(req.headers.get(HttpHeaders.Names.AUTHORIZATION)) flatMap {
        case AuthHeader(token) => token map { Base64StringEncoder.decode(_) }
        case _ => None
      }

    protected[this] def setWwwAuthenticate(rsp: HttpResponse, auth: String): HttpResponse = {
      rsp.headers.set(HttpHeaders.Names.WWW_AUTHENTICATE, auth)
      rsp
    }

    /** Return a spnego 'challenge' */
    protected[this] def unauthorized(req: HttpRequest): HttpResponse =
      setWwwAuthenticate(
        new DefaultHttpResponse(req.getProtocolVersion, Status.Unauthorized),
        AuthScheme)

    protected[this] def authenticated(req: HttpRequest, context: GSSContext): Authenticated.Http =
      Authenticated.Http(req, context)
  }

  class RichHttpFilter(
    loginContext: String,
    principal: Option[GSSName] = None,
    oid: Option[Oid] = None,
    manager: GSSManager = GSSManager.getInstance,
    pool: FuturePool = FuturePool.unboundedPool)
  extends SpnegoAuthenticator[Request, Response](
    loginContext, principal, oid, manager, pool) {

    protected[this] def getNegotiation(req: Request): Option[Array[Byte]] =
      req.authorization flatMap {
        case AuthHeader(token) => token map { Base64StringEncoder.decode(_) }
        case _ => None
      }

    protected[this] def setWwwAuthenticate(rsp: Response, auth: String): Response = {
      rsp.wwwAuthenticate = auth
      rsp
    }

    protected[this] def unauthorized(req: Request): Response = {
      val rsp = req.response
      rsp.status = Status.Unauthorized
      setWwwAuthenticate(rsp, AuthScheme)
    }

    protected[this] def authenticated(req: Request, context: GSSContext): Authenticated.RichHttp =
      Authenticated.RichHttp(req, context)
  }
}

/**
 * A SPNEGO HTTP authenticator as defined in https://tools.ietf.org/html/rfc4559.
 *
 * JAAS is used to establish the server's credentials (via the configuration section named by
 * loginContext).  If a principal or OID is provided, it is used to override the settings in the JAAS
 * configuration file.
 *
 * Since the authentication operations may block when i.e. talking to a KDC, these potentially
 * blocking calls are wrapped in a FuturePool.
 */
abstract class SpnegoAuthenticator[Req, Rsp](
  loginContext: String,
  principal: Option[GSSName],
  oid: Option[Oid],
  manager: GSSManager,
  pool: FuturePool)
         extends Filter[Req, Rsp, SpnegoAuthenticator.Authenticated[Req], Rsp] {

  import SpnegoAuthenticator._

  /** If the request contained a valid spnego negotiation, return it. */
  protected[this] def getNegotiation(req: Req): Option[Array[Byte]]

  /** Add a WWW-Authenticate: Negotiate <token> header to the response. */
  protected[this] def setWwwAuthenticate(rsp: Rsp, auth: String): Rsp

  /** Create an unauthorized response */
  protected[this] def unauthorized(req: Req): Rsp

  protected[this] def authenticated(req: Req, context: GSSContext): Authenticated[Req]

  protected[this] def loadCredential(): Future[GSSContext] =
    pool {
      log.debug("Getting credential: %s", principal)
      val portal = new LoginContext(loginContext)
      portal.login()
      Subject.doAs(portal.getSubject, new PrivilegedAction[GSSContext] {
        def run(): GSSContext = {
          manager.createContext(
            manager.createCredential(
              principal.orNull,
              GSSCredential.DEFAULT_LIFETIME,
              oid.orNull,
              GSSCredential.ACCEPT_ONLY))
        }
      })
    } onSuccess { cred =>
      log.debug("Got credential: %s", cred)
    }

  protected[this] case class Negotiated(
    established: Option[GSSContext],
    wwwAuthenticate: Option[String])

  protected[this] def accept(context: GSSContext, negotiation: Array[Byte]): Future[Negotiated] =
    pool {
      val token = context.acceptSecContext(negotiation, 0, negotiation.length)
      val established = if (context.isEstablished) Some(context) else None
      val wwwAuthenticate = Option(token) map { AuthScheme + " " + Base64StringEncoder.encode(_) }
      Negotiated(established, wwwAuthenticate)
    }

  def apply(req: Req,  authed: Service[Authenticated[Req], Rsp]): Future[Rsp] =
    getNegotiation(req) map { negotiation =>
      loadCredential() flatMap {
        accept(_, negotiation)
      } flatMap { negotiated =>
        negotiated.established map { ctx =>
          authed(authenticated(req, ctx))
        } getOrElse {
          Future value unauthorized(req)
        } map { rsp =>
          negotiated.wwwAuthenticate map {
            setWwwAuthenticate(rsp, _)
          } getOrElse rsp
        }
      } handle {
        case e: GSSException => {
          log.error(e, "authenticating")
          unauthorized(req)
        }
      }
    } getOrElse {
      Future value unauthorized(req)
    }
}
