package com.twitter.finagle.ssl.server

import com.twitter.finagle.Stack
import com.twitter.finagle.ssl.{ClientAuth, Engine, SslConfigurations}
import javax.net.ssl.{SSLContext, SSLEngine}

/**
 * Instances of this class provide a method to create server Finagle
 * [[Engine Engines]] for use with TLS.
 */
abstract class SslServerEngineFactory {

  /**
   * Creates a new [[Engine]] based on an
   * [[SslServerConfiguration]].
   *
   * @param config A collection of parameters which the
   * engine factory should consider when creating the
   * TLS server [[Engine]].
   */
  def apply(config: SslServerConfiguration): Engine
}

object SslServerEngineFactory {

  /**
   * $param the server engine factory used for creating an [[Engine]]
   * which is used with an SSL/TLS connection.
   *
   * @param factory The [[SslServerEngineFactory]] to use for creating
   * an [[Engine]] based off of an [[SslServerConfiguration]].
   *
   * @note By default a [[JdkServerEngineFactory]] will be used if this
   * param is not configured.
   */
  case class Param(factory: SslServerEngineFactory) {
    def mk(): (Param, Stack.Param[Param]) =
      (this, Param.param)
  }
  object Param {
    implicit val param = Stack.Param(Param(JdkServerEngineFactory))
  }

  def configureClientAuth(sslEngine: SSLEngine, clientAuth: ClientAuth): Unit = clientAuth match {
    case ClientAuth.Unspecified => // Do Nothing
    case ClientAuth.Off => sslEngine.setWantClientAuth(false)
    case ClientAuth.Wanted => sslEngine.setWantClientAuth(true)
    case ClientAuth.Needed => sslEngine.setNeedClientAuth(true)
  }

  /**
   * Configure the supplied [[Engine Engine's]] client mode,
   * protocols, cipher suites, and client authentication.
   */
  def configureEngine(engine: Engine, config: SslServerConfiguration): Unit = {
    val sslEngine = engine.self

    // Use false here to ensure that this engine is seen as a server engine.
    // This matters for handshaking.
    sslEngine.setUseClientMode(false)

    SslConfigurations.configureProtocols(sslEngine, config.protocols)
    SslConfigurations.configureCipherSuites(sslEngine, config.cipherSuites)
    configureClientAuth(sslEngine, config.clientAuth)
  }

  /*
   * Use the supplied `javax.net.ssl.SSLContext` to create a new
   * [[Engine]].
   */
  def createEngine(sslContext: SSLContext): Engine = {
    val sslEngine = sslContext.createSSLEngine()
    new Engine(sslEngine)
  }

}
