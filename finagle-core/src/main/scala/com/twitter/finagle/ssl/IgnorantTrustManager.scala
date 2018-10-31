package com.twitter.finagle.ssl

import java.net.Socket
import java.security.cert.X509Certificate
import javax.net.ssl.{SSLEngine, X509ExtendedTrustManager}

/**
 * A trust manager that does not validate anything.
 */
private[ssl] class IgnorantTrustManager extends X509ExtendedTrustManager {
  def getAcceptedIssuers(): Array[X509Certificate] = Array.empty

  def checkClientTrusted(chain: Array[X509Certificate], authType: String): Unit = {
    // Do nothing.
  }

  def checkClientTrusted(chain: Array[X509Certificate], authType: String, socket: Socket): Unit = {
    // Do Nothing.
  }

  def checkClientTrusted(
    chain: Array[X509Certificate],
    authType: String,
    engine: SSLEngine
  ): Unit = {
    // Do Nothing.
  }

  def checkServerTrusted(chain: Array[X509Certificate], authType: String): Unit = {
    // Do nothing.
  }

  def checkServerTrusted(chain: Array[X509Certificate], authType: String, socket: Socket): Unit = {
    // Do Nothing.
  }

  def checkServerTrusted(
    chain: Array[X509Certificate],
    authType: String,
    engine: SSLEngine
  ): Unit = {
    // Do Nothing.
  }
}
