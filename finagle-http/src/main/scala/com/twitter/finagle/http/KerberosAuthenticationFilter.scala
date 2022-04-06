package com.twitter.finagle.http

import com.twitter.finagle.http.SpnegoAuthenticator.ClientFilter
import com.twitter.finagle.http.SpnegoAuthenticator.Credentials
import com.twitter.finagle.http.SpnegoAuthenticator.ServerFilter
import com.twitter.finagle.http.param.ClientKerberos
import com.twitter.finagle.http.param.ClientKerberosConfiguration
import com.twitter.finagle.http.param.KerberosConfiguration
import com.twitter.finagle.http.param.ServerKerberos
import com.twitter.finagle.http.param.ServerKerberosConfiguration
import com.twitter.finagle.Filter
import com.twitter.finagle.Service
import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.Stack
import com.twitter.finagle.Stackable
import com.twitter.util.Future
import com.twitter.util.FuturePool
import java.io.File
import java.io.FileOutputStream
import java.io.PrintWriter
import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.Path
import javax.security.auth.login.Configuration

object AuthenticatedIdentityContext {
  private val AuthenticatedIdentityNotSet = "AUTH_USER_NOT_SET"
  private val AuthenticatedIdentity = Request.Schema.newField[String](AuthenticatedIdentityNotSet)

  /**
   * @see AuthenticatedIdentity [[https://web.mit.edu/kerberos/krb5-1.5/krb5-1.5.4/doc/krb5-user/What-is-a-Kerberos-Principal_003f.html]]
   */
  implicit class AuthenticatedIdentityContextSyntax(val request: Request) extends AnyVal {
    def authenticatedIdentity: String = request.ctx(AuthenticatedIdentity).takeWhile(_ != '@')
  }

  private[http] def setUser(request: Request, username: String): Unit = {
    request.ctx.updateAndLock(AuthenticatedIdentity, username)
  }
}

/**
 * Apply filter asynchronously
 */
private[finagle] class AsyncFilter[Req, IntermediateReq, Rep](
  async: Future[Filter[Req, Rep, IntermediateReq, Rep]])
    extends Filter[Req, Rep, IntermediateReq, Rep] {
  def apply(req: Req, svc: Service[IntermediateReq, Rep]): Future[Rep] = {
    async.flatMap(_.apply(req, svc))
  }
}

/**
 * Apply kerberos authentication to http requests.
 */
object KerberosAuthenticationFilter {
  val role: Stack.Role = Stack.Role("KerberosAuthentication")

  /**
   * Kerberos server module to apply Spnego server filter
   */
  def serverModule: Stackable[ServiceFactory[Request, Response]] = {
    new Stack.Module1[ServerKerberos, ServiceFactory[
      Request,
      Response
    ]] {
      val role: Stack.Role = KerberosAuthenticationFilter.role
      val description = "Add kerberos server authentication to requests"
      def make(
        serverKerberos: ServerKerberos,
        next: ServiceFactory[Request, Response]
      ): ServiceFactory[Request, Response] = {
        if (serverKerberos.serverKerberosConfiguration.authEnabled) {
          new AsyncFilter[Request, SpnegoAuthenticator.Authenticated[Request], Response](
            SpnegoServerFilter(serverKerberos.serverKerberosConfiguration))
            .andThen(ExtractAuthAndCatchUnauthorized).andThen(next)
        } else next
      }
    }
  }

  /**
   * Kerberos client module to apply Spnego client filter
   */
  def clientModule: Stackable[ServiceFactory[Request, Response]] = {
    new Stack.Module1[ClientKerberos, ServiceFactory[
      Request,
      Response
    ]] {
      val role: Stack.Role = KerberosAuthenticationFilter.role
      val description = "Add kerberos client authentication to requests"
      def make(
        clientKerberos: ClientKerberos,
        next: ServiceFactory[Request, Response]
      ): ServiceFactory[Request, Response] = {
        if (clientKerberos.clientKerberosConfiguration.authEnabled) {
          new AsyncFilter[Request, Request, Response](
            SpnegoClientFilter(clientKerberos.clientKerberosConfiguration))
            .andThen(next)
        } else next
      }
    }
  }

  /**
   * Create/append a jaas config and set it to the system property
   * @param kerberosConfiguration Kerberos jaas configuration
   * @param loginContext Kerberos Login context
   * @param jaasDir the (optional) directory where the jaas config will be written to.
   *                otherwise, the config will be written to the current working directory.
   * @see JaasConfiguration [[https://docs.oracle.com/javase/7/docs/jre/api/security/jaas/spec/com/sun/security/auth/module/Krb5LoginModule.html]]
   */
  private[this] def jaas(
    kerberosConfiguration: KerberosConfiguration,
    loginContext: String,
    jaasDir: Option[Path]
  ): Unit = {
    val jaas = "jaas-internal.conf"
    val jaasConfiguration =
      s"""$loginContext {
         |  com.sun.security.auth.module.Krb5LoginModule required
         |  keyTab="${kerberosConfiguration.keyTab.get}"
         |  principal="${kerberosConfiguration.principal.get}"
         |  useKeyTab=${kerberosConfiguration.useKeyTab}
         |  storeKey=${kerberosConfiguration.storeKey}
         |  refreshKrb5Config=${kerberosConfiguration.refreshKrb5Config}
         |  debug=${kerberosConfiguration.debug}
         |  doNotPrompt=${kerberosConfiguration.doNotPrompt}
         |  authEnabled=${kerberosConfiguration.authEnabled}; 
         |};""".stripMargin
    val jaasFilePath = jaasDir
      .map(_.resolve(jaas))
      .getOrElse(FileSystems.getDefault.getPath(jaas))
      .toAbsolutePath
    if (Files.exists(jaasFilePath)) {
      if (!Files.lines(jaasFilePath).anyMatch(line => line.equals(s"$loginContext {"))) {
        writeFile(jaasFilePath, s"\n\n$jaasConfiguration", true)
        Configuration.getConfiguration.refresh()
      }
    } else writeFile(jaasFilePath, jaasConfiguration)
    System.setProperty("java.security.auth.login.config", jaasFilePath.toString)
  }

  private[this] def writeFile(path: Path, content: String, append: Boolean = false) =
    new PrintWriter(new FileOutputStream(new File(path.toUri), append)) {
      write(content)
      close()
    }

  /**
   * A finagle kerberos authentication filter.
   * This calls an underlying finagle filter (SpnegoAuthenticator) and then applies a second "conversion"
   * filter to convert the request to a request object that is compatible with finagle.
   * Applies server kerberos filter to wrap the spnego server filter with default standard jaas config
   */
  private[finagle] object SpnegoServerFilter {
    def apply(
      serverKerberosConfiguration: ServerKerberosConfiguration
    ): Future[Filter[Request, Response, SpnegoAuthenticator.Authenticated[Request], Response]] =
      apply(serverKerberosConfiguration, None)

    def apply(
      serverKerberosConfiguration: ServerKerberosConfiguration,
      jaasDir: Option[Path]
    ): Future[Filter[Request, Response, SpnegoAuthenticator.Authenticated[Request], Response]] =
      FuturePool.unboundedPool {
        jaas(serverKerberosConfiguration, "kerberos-http-server", jaasDir)
        ServerFilter(new Credentials.JAASServerSource("kerberos-http-server"))
      }
  }

  /**
   * Applies client kerberos filter to wrap the spnego client filter with default standard jaas config
   */
  private[finagle] object SpnegoClientFilter {
    def apply(
      clientKerberosConfiguration: ClientKerberosConfiguration
    ): Future[Filter[Request, Response, Request, Response]] =
      apply(clientKerberosConfiguration, None)

    def apply(
      clientKerberosConfiguration: ClientKerberosConfiguration,
      jaasDir: Option[Path]
    ): Future[Filter[Request, Response, Request, Response]] = FuturePool.unboundedPool {
      jaas(clientKerberosConfiguration, "kerberos-http-client", jaasDir)
      ClientFilter(
        new Credentials.JAASClientSource(
          "kerberos-http-client",
          clientKerberosConfiguration.serverPrincipal.get))
    }
  }

  private[finagle] object ExtractAuthAndCatchUnauthorized
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
}
