package com.twitter.finagle.ssl

import com.twitter.finagle.SslException

/**
 * SslConfigurationException is an exception which is thrown by
 * a particular engine factory when the engine factory does not
 * support the collection of parameters specified by the
 * [[SslClientConfiguration]] or the [[SslServerConfiguration]].
 */
case class SslConfigurationException(cause: Throwable) extends SslException(Option(cause), None)

private[finagle] object SslConfigurationException {

  def notSupported(featureName: String, engineFactoryName: String): SslConfigurationException =
    SslConfigurationException(
      new IllegalArgumentException(
        s"$featureName is not supported at this time for $engineFactoryName"
      )
    )

}
