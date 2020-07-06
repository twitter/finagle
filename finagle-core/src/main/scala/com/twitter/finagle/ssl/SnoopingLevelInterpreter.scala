package com.twitter.finagle.ssl

import com.twitter.finagle.Stack
import com.twitter.finagle.ssl.server.SslServerConfiguration
import com.twitter.finagle.transport.Transport
import com.twitter.logging.Logger

private[finagle] object SnoopingLevelInterpreter {

  private val logger = Logger.get()

  case class Param(interpreter: Interpreter)

  object Param {
    implicit lazy val param: Stack.Param[Param] = Stack.Param(Param(Disabled))
  }

  sealed abstract class Interpreter

  /** TLS snooping disabled */
  case object Disabled extends Interpreter

  /** TLS snooping is enabled based on the predicate function */
  case class Enabled(predicate: (OpportunisticTls.Level, SslServerConfiguration) => Boolean)
      extends Interpreter

  /**
   * TLS Snooping configuration for protocols that don't support negotiation.
   *
   * Interprets stack params for non-negotiating protocols (HTTP, for example).
   * For non-negotiating protocols there isn't a secondary way of making sure that
   * a cleartext connection later gets upgraded. As such 'Required' doesn't enable
   * snooping as it expresses that encryption is required, and snooping would make
   * it optional.
   *
   * Truth matrix:
   *                         Level
   *               Off   |  Desired | Required
   * - Client Auth ------------------------------
   *  Off       |  Off   |    On    |  Off
   * Wanted     |  Off   |    On    |  Off
   * Needed     |  Off   |    Off   |  Off
   */
  val EnabledForNonNegotiatingProtocols: Param = Param(Enabled(nonNegotiating))

  /**
   * TLS Snooping configuration for protocols that support negotiation.
   *
   * @note that this configuration will allow cleartext connections to proceed even if
   *       the `OpportunisticTls.Level` is `Required` and it is the responsibility of the
   *       protocol to reject a cleartext session if the configuration is incompatible
   *       with security requirements.
   *
   * Interprets stack params for protocols that can negotiate TLS (Mux, for example).
   * For these protocols we want to enable TLS either eagerly, or as part of a cleartext
   * negotiation phase. Note that it becomes the the sessions responsibility to reject
   * cleartext connections that don't negotiate to the required security level.
   *
   * Truth matrix:
   *
   *                             Level
   *                   Off   |  Desired | Required
   * - Client Auth ------------------------------
   *      Off       |  Off   |    On    |  On
   *     Wanted     |  Off   |    On    |  On
   *     Needed     |  Off   |    On    |  On
   */
  val EnabledForNegotiatingProtocols: Param = Param(Enabled(withNegotiating))

  /**
   * Determine whether TLS snooping should be enabled based on the stack params.
   */
  def shouldEnableSnooping(params: Stack.Params): Boolean = {
    params[SnoopingLevelInterpreter.Param].interpreter match {
      case Disabled => false
      case Enabled(enableSnooping) =>
        val level = params[OpportunisticTls.Param].level
        params[Transport.ServerSsl].sslServerConfiguration match {
          case Some(config) =>
            enableSnooping(level, config)

          case None =>
            if (level != OpportunisticTls.Off) {
              logger.warning(
                "Tls snooping was potentially desired but not enabled because a security " +
                  "configuration was not specified")
            }
            false
        }
    }
  }

  private def nonNegotiating(
    level: OpportunisticTls.Level,
    sslServerConfiguration: SslServerConfiguration
  ): Boolean = {
    // We want to make sure that we both desire opportunistic TLS and haven't
    // signaled through the client auth param that we want to verify the peer.
    if (level != OpportunisticTls.Desired) false
    else if (sslServerConfiguration.clientAuth != ClientAuth.Needed) true
    else {
      logger.warning(
        "Opportunistic Tls was desired but not enabled because client authorization required.")
      false
    }
  }

  private def withNegotiating(
    level: OpportunisticTls.Level,
    sslServerConfiguration: SslServerConfiguration
  ): Boolean = {
    level != OpportunisticTls.Off
  }
}
