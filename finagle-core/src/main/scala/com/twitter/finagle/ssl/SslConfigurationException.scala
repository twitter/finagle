package com.twitter.finagle.ssl

/**
 * SslConfigurationException is an exception which is thrown by
 * a particular engine factory when the engine factory does not
 * support the collection of parameters specified by the
 * [[SslClientConfiguration]] or the [[SslServerConfiguration]].
 */
case class SslConfigurationException(
    msg: String,
    cause: Throwable)
  extends Exception(msg, cause)

private[finagle] object SslConfigurationException {

  def notSupported(featureName: String, engineFactoryName: String): SslConfigurationException = {
    SslConfigurationException(s"$featureName is not supported at this time for $engineFactoryName", null)
  }

}
