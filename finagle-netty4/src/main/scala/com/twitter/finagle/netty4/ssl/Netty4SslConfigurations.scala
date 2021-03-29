package com.twitter.finagle.netty4.ssl

import com.twitter.finagle.ssl.{ApplicationProtocols, SslConfigurationException, TrustCredentials}
import com.twitter.util.{Return, Throw, Try}
import io.netty.handler.ssl.{ApplicationProtocolConfig, SslContextBuilder, SslProvider}
import io.netty.handler.ssl.ApplicationProtocolConfig.{
  Protocol,
  SelectedListenerFailureBehavior,
  SelectorFailureBehavior
}
import io.netty.handler.ssl.util.InsecureTrustManagerFactory
import scala.collection.JavaConverters._
import scala.util.control.NonFatal

/**
 * Convenience functions for setting values on a Netty `SslContextBuilder`
 * which are applicable to both client and server engines.
 */
private[finagle] object Netty4SslConfigurations {

  /**
   * Configures the trust credentials of the `SslContextBuilder`. This
   * method mutates the `SslContextBuilder`, and returns it as the result.
   *
   * @note TrustCredentials.Unspecified does not change the builder,
   */
  def configureTrust(
    builder: SslContextBuilder,
    trustCredentials: TrustCredentials
  ): SslContextBuilder = {
    trustCredentials match {
      case TrustCredentials.Unspecified =>
        builder // Do Nothing
      case TrustCredentials.Insecure =>
        builder.trustManager(InsecureTrustManagerFactory.INSTANCE)
      case TrustCredentials.CertCollection(file) =>
        builder.trustManager(file)
      case TrustCredentials.X509Certificates(x509Certs) =>
        builder.trustManager(x509Certs: _*)
      case TrustCredentials.TrustManagerFactory(trustManagerFactory) =>
        builder.trustManager(trustManagerFactory)
    }
  }

  /**
   * Configures the application protocols of the `SslContextBuilder`. This
   * method mutates the `SslContextBuilder`, and returns it as the result.
   *
   * @note This also sets the `SelectorFailureBehavior` to NO_ADVERTISE,
   * and the `SelectedListenerFailureBehavior` to ACCEPT as those are the
   * only modes supported by both JDK and Native engines.
   */
  def configureApplicationProtocols(
    builder: SslContextBuilder,
    applicationProtocols: ApplicationProtocols,
    negotiationProtocol: Protocol
  ): SslContextBuilder = {
    applicationProtocols match {
      case ApplicationProtocols.Unspecified =>
        builder // Do Nothing
      case ApplicationProtocols.Supported(protos) =>
        builder.applicationProtocolConfig(
          new ApplicationProtocolConfig(
            negotiationProtocol,
            SelectorFailureBehavior.NO_ADVERTISE,
            SelectedListenerFailureBehavior.ACCEPT,
            protos.asJava
          )
        )
    }
  }

  /**
   * Configures the SSL provider with the JDK SSL provider if `forceJDK` is true.
   *
   * @note This is necessary in environments where the native engine could fail to load.
   */
  def configureProvider(builder: SslContextBuilder, forceJdk: Boolean): SslContextBuilder =
    if (forceJdk) builder.sslProvider(SslProvider.JDK)
    else builder.sslProvider(SslProvider.OPENSSL_REFCNT)

  /**
   * Unwraps the `Try[SslContextBuilder]` and throws an `SslConfigurationException` for
   * `NonFatal` errors.
   */
  def unwrapTryContextBuilder(builder: Try[SslContextBuilder]): SslContextBuilder =
    builder match {
      case Return(sslContextBuilder) =>
        sslContextBuilder
      case Throw(NonFatal(nonFatal)) =>
        throw new SslConfigurationException(nonFatal)
      case Throw(throwable) =>
        throw throwable
    }

}
