package com.twitter.finagle.netty4.ssl.server

import java.net.Socket
import java.security.cert.X509Certificate
import javax.net.ssl.SSLEngine
import javax.net.ssl.X509ExtendedTrustManager
import javax.net.ssl.X509TrustManager

/**
 * Context for storing service identifiers extracted during SSL handshake verification.
 * This is used to preserve the client service identifier even when certificate validation fails.
 */
object ServiceIdentifierContext {
  private val threadLocal = new ThreadLocal[Option[String]]()

  def set(serviceId: Option[String]): Unit = threadLocal.set(serviceId)
  def get(): Option[String] = Option(threadLocal.get()).flatten
  def clear(): Unit = threadLocal.remove()
}

/**
 * A trust manager that wraps another trust manager and captures the service identifier
 * from client certificates during verification, before validation occurs.
 *
 * This allows us to track which clients are failing SSL handshakes, even when their
 * certificates fail PKIX path building validation.
 */
class ServiceIdentifierCapturingTrustManager(delegate: X509TrustManager)
    extends X509ExtendedTrustManager {

  /**
   * Common logic to capture service identifier and delegate to trust manager.
   * Called by all checkClientTrusted overloads.
   */
  private def captureAndVerify(chain: Array[X509Certificate])(verify: => Unit): Unit = {
    ServiceIdentifierContext.set(ServiceIdentifierExtractor.extractServiceIdentifier(chain))
    try {
      verify
    } finally {
      // Don't clear here - let the verification handler retrieve it
    }
  }

  // Base X509TrustManager method
  override def checkClientTrusted(chain: Array[X509Certificate], authType: String): Unit = {
    captureAndVerify(chain) {
      delegate.checkClientTrusted(chain, authType)
    }
  }

  // X509ExtendedTrustManager Socket overload
  override def checkClientTrusted(
    chain: Array[X509Certificate],
    authType: String,
    socket: Socket
  ): Unit = {
    captureAndVerify(chain) {
      delegate match {
        case extDelegate: X509ExtendedTrustManager =>
          extDelegate.checkClientTrusted(chain, authType, socket)
        case _ =>
          delegate.checkClientTrusted(chain, authType)
      }
    }
  }

  // X509ExtendedTrustManager SSLEngine overload (used by Netty with OpenSSL)
  override def checkClientTrusted(
    chain: Array[X509Certificate],
    authType: String,
    engine: SSLEngine
  ): Unit = {
    captureAndVerify(chain) {
      delegate match {
        case extDelegate: X509ExtendedTrustManager =>
          extDelegate.checkClientTrusted(chain, authType, engine)
        case _ =>
          delegate.checkClientTrusted(chain, authType)
      }
    }
  }

  // Server trust verification - we don't capture service IDs here since we're the server
  // These methods validate outbound connections (when the server acts as a client)
  override def checkServerTrusted(chain: Array[X509Certificate], authType: String): Unit = {
    delegate.checkServerTrusted(chain, authType)
  }

  override def checkServerTrusted(
    chain: Array[X509Certificate],
    authType: String,
    socket: Socket
  ): Unit = {
    delegate match {
      case extDelegate: X509ExtendedTrustManager =>
        extDelegate.checkServerTrusted(chain, authType, socket)
      case _ =>
        delegate.checkServerTrusted(chain, authType)
    }
  }

  override def checkServerTrusted(
    chain: Array[X509Certificate],
    authType: String,
    engine: SSLEngine
  ): Unit = {
    delegate match {
      case extDelegate: X509ExtendedTrustManager =>
        extDelegate.checkServerTrusted(chain, authType, engine)
      case _ =>
        delegate.checkServerTrusted(chain, authType)
    }
  }

  override def getAcceptedIssuers: Array[X509Certificate] = delegate.getAcceptedIssuers
}
