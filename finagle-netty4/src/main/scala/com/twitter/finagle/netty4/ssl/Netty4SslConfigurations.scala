package com.twitter.finagle.netty4.ssl

import com.twitter.finagle.ssl.{ApplicationProtocols, TrustCredentials}
import io.netty.handler.ssl.{ApplicationProtocolConfig, SslContextBuilder, SslProvider}
import io.netty.handler.ssl.ApplicationProtocolConfig.{
  Protocol, SelectedListenerFailureBehavior, SelectorFailureBehavior}
import io.netty.handler.ssl.util.InsecureTrustManagerFactory
import scala.collection.JavaConverters._

/**
 * Convenience functions for setting values on a Netty `SslContextBuilder`
 * which are applicable to both client and server engines.
 */
private[ssl] object Netty4SslConfigurations {

  /**
   * Configures the trust credentials of the `SslContextBuilder`. This
   * method mutates the `SslContextBuilder`, and returns it as the result.
   *
   * @note TrustCredentials.Unspecified does not change the builder,
   * as it is not possible to set the trustManager to use the system
   * trust credentials, like with the JDK engine factories.
   *
   * @note TrustCredentials.Insecure forces the `SslProvider` of the
   * `SslContextBuilder` to be a JDK instance, as Netty's
   * `InsecureTrustManagerFactory` is not supported for native engines.
   */
  def configureTrust(
    builder: SslContextBuilder,
    trustCredentials: TrustCredentials
  ): SslContextBuilder = {
    trustCredentials match {
      case TrustCredentials.Unspecified =>
        builder // Do Nothing
      case TrustCredentials.Insecure =>
        builder
          .sslProvider(SslProvider.JDK)
          .trustManager(InsecureTrustManagerFactory.INSTANCE)
      case TrustCredentials.CertCollection(file) =>
        builder.trustManager(file)
    }
  }

  /**
   * Configures the application protocols of the `SslContextBuilder`. This
   * method mutates the `SslContextBuilder`, and returns it as the result.
   *
   * @note This sets which application level protocol negotiation to
   * use NPN and ALPN.
   *
   * @note This also sets the `SelectorFailureBehavior` to NO_ADVERTISE,
   * and the `SelectedListenerFailureBehavior` to ACCEPT as those are the
   * only modes supported by both JDK and Native engines.
   */
  def configureApplicationProtocols(
    builder: SslContextBuilder,
    applicationProtocols: ApplicationProtocols
  ): SslContextBuilder = {
    applicationProtocols match {
      case ApplicationProtocols.Unspecified =>
        builder // Do Nothing
      case ApplicationProtocols.Supported(protos) =>
        builder.applicationProtocolConfig(
          new ApplicationProtocolConfig(
            Protocol.NPN_AND_ALPN,
            // NO_ADVERTISE and ACCEPT are the only modes supported by both OpenSSL and JDK SSL.
            SelectorFailureBehavior.NO_ADVERTISE,
            SelectedListenerFailureBehavior.ACCEPT,
            protos.asJava))
    }
  }

  /**
   * Configures the SSL provider with the JDK SSL provider if `forceJDK` is true.
   * 
   * @note This is necessary in environments where the native engine could fail to load.
   */
  def configureProvider(
    builder: SslContextBuilder,
    forceJdk: Boolean
  ): SslContextBuilder =
    if (forceJdk) builder.sslProvider(SslProvider.JDK)
    else builder

}
