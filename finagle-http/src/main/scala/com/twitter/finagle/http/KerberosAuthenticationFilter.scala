package com.twitter.finagle.http

import com.twitter.finagle.http.SpnegoAuthenticator.{Credentials, ServerFilter}
import com.twitter.finagle.http.param.{Kerberos, KerberosConfiguration}
import com.twitter.finagle.{Filter, Service, ServiceFactory, SimpleFilter, Stack, Stackable}
import com.twitter.util.{Future, FuturePool}
import java.io.PrintWriter
import java.nio.file.FileSystems

object AuthenticatedIdentityContext {
  private val AuthenticatedIdentityNotSet = "AUTH_USER_NOT_SET"
  private val AuthenticatedIdentity = Request.Schema.newField[String](AuthenticatedIdentityNotSet)

  /**
   * @see AuthenticatedIdentity [[https://web.mit.edu/kerberos/krb5-1.5/krb5-1.5.4/doc/krb5-user/What-is-a-Kerberos-Principal_003f.html]]
   */
  implicit class AuthenticatedIdentityContextSyntax(val request: Request) extends AnyVal {
    def authenticatedIdentity: String = request.ctx(AuthenticatedIdentity).takeWhile(_ != '@')
  }

  private[twitter] def setUser(request: Request, username: String): Unit = {
    request.ctx.updateAndLock(AuthenticatedIdentity, username)
  }
}

object ExtractAuthAndCatchUnauthorized
    extends Filter[SpnegoAuthenticator.Authenticated[Request], Response, Request, Response] {
  def apply(
    req: SpnegoAuthenticator.Authenticated[Request],
    svc: Service[Request, Response]
  ): Future[Response] = {
    val httpRequest = req.request
    AuthenticatedIdentityContext.setUser(httpRequest, req.context.getSrcName.toString)
    svc(httpRequest).map { resp =>
      if (resp.status == Status.Unauthorized) {
        resp.contentType = "application/json; charset=utf-8"
        resp.setContentString("""
            {
              "error": "You are not authenticated."
            }
            """)
      }
      resp
    }
  }
}

/**
 * A finagle authentication filter.
 * This calls an underlying finagle filter (SpnegoAuthenticator) and then applies a second "conversion"
 * filter to convert the request to a request object that is compatible with finagle.
 * @see JaasConfiguration [[https://docs.oracle.com/javase/7/docs/jre/api/security/jaas/spec/com/sun/security/auth/module/Krb5LoginModule.html]]
 */
object Spnego {
  private def pool: FuturePool = FuturePool.unboundedPool
  def apply(kerberosConfiguration: KerberosConfiguration): Future[ServerFilter] = pool {
    val jaas = "jaas.conf"
    val jaasConfiguration =
      s"""kerberos-http { 
         |com.sun.security.auth.module.Krb5LoginModule required
         | keyTab="${kerberosConfiguration.keyTab.get}"
         | principal="${kerberosConfiguration.principal.get}"
         | useKeyTab=${kerberosConfiguration.useKeyTab}
         | storeKey=${kerberosConfiguration.storeKey}                   
         | refreshKrb5Config=${kerberosConfiguration.refreshKrb5Config}      
         | debug=${kerberosConfiguration.debug}
         | doNotPrompt=${kerberosConfiguration.doNotPrompt}
         | authEnabled=${kerberosConfiguration.authEnabled}; 
         | };""".stripMargin
    new PrintWriter(jaas) {
      write(jaasConfiguration)
      close()
    }
    val jaasFilePath = FileSystems.getDefault.getPath(jaas).toAbsolutePath.toString
    System.setProperty("java.security.auth.login.config", jaasFilePath)
    ServerFilter(new Credentials.JAASServerSource("kerberos-http"))
  }
}

/**
 * Chain Spnego async filter with extractAuthAndCatchUnauthorized filter
 */
private[finagle] class AndThenAsync[Req, IntermediateReq, Rep](
  async: Future[Filter[Req, Rep, IntermediateReq, Rep]],
  other: Filter[IntermediateReq, Rep, Req, Rep])
    extends SimpleFilter[Req, Rep] {
  private val composedFilters: Future[Filter[Req, Rep, Req, Rep]] =
    async.map(_.andThen(other))
  def apply(req: Req, svc: Service[Req, Rep]): Future[Rep] = {
    composedFilters.flatMap(_.apply(req, svc))
  }
}

/**
 * Adds kerberos authentication to http requests.
 */
object KerberosAuthenticationFilter {
  val role: Stack.Role = Stack.Role("KerberosAuthentication")
  def module: Stackable[ServiceFactory[Request, Response]] = {
    new Stack.Module1[Kerberos, ServiceFactory[
      Request,
      Response
    ]] {
      val role: Stack.Role = KerberosAuthenticationFilter.role
      val description = "Add kerberos authentication to requests"
      def make(
        krb: Kerberos,
        next: ServiceFactory[Request, Response]
      ): ServiceFactory[Request, Response] = {
        if (krb.kerberosConfiguration.principal.nonEmpty && krb.kerberosConfiguration.keyTab.nonEmpty && krb.kerberosConfiguration.authEnabled) {
          new AndThenAsync[Request, SpnegoAuthenticator.Authenticated[Request], Response](
            Spnego(krb.kerberosConfiguration),
            ExtractAuthAndCatchUnauthorized).andThen(next)
        } else next
      }
    }
  }
}
