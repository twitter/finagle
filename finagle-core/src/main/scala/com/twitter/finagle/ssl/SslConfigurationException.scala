package com.twitter.finagle.ssl

/**
 * SslConfigurationException is an exception which is thrown by
 * a particular engine factory when the engine factory does not
 * support the collection of parameters specified by the
 * [[SslClientConfiguration]] or the [[SslServerConfiguration]].
 */
private[ssl] case class SslConfigurationException(msg: String) extends Exception(msg)
