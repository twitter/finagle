package com.twitter.finagle.netty4.ssl.server

import java.net.URI
import java.security.cert.X509Certificate
import javax.security.auth.x500.X500Principal
import scala.util.control.NonFatal

/**
 * Utility for extracting service identifiers from X.509 certificates.
 * Supports both SAN URI (preferred) and Common Name (CN) fallback extraction.
 */
object ServiceIdentifierExtractor {

  // SAN type constants from X.509
  private val SAN_URI_TYPE = 6
  private val SAN_DNS_TYPE = 2

  // Regex to match service identifier in Common Name (CN)
  private val ServiceIdentifierRegex =
    "^(?:twtr:svc:)?([a-zA-Z0-9_-]+:[a-zA-Z0-9_-]+:[a-zA-Z0-9_-]+:[a-zA-Z0-9_-]+:[a-zA-Z0-9_-]+(?::[a-zA-Z0-9_-]+)?(?:\\.[a-zA-Z0-9_-]+)*)$".r

  /**
   * Extracts service identifier from an X.509 certificate chain.
   *
   * Extraction order:
   * 1. SAN URI (type 6) with scheme "twtr:" (preferred per RFC 5280)
   * 2. Common Name (CN) matching service identifier pattern
   * 3. Non-mTLS fallback for other CN values
   *
   * Expected format: twtr:svc:role:service:environment:zone
   *
   * @param chain Array of certificates (uses first certificate)
   * @return Optional service identifier string, or fallback value
   */
  def extractServiceIdentifier(chain: Array[X509Certificate]): Option[String] = {
    if (chain == null || chain.isEmpty) {
      return Some("no_peer_cert")
    }

    try {
      val cert = chain(0)

      // Try SAN URI first (preferred per RFC 5280)
      val serviceIdFromSan = Option(cert.getSubjectAlternativeNames).flatMap { sans =>
        import scala.collection.JavaConverters._
        sans.asScala.collectFirst {
          case list if list.size() >= 2 && list.get(0) == SAN_URI_TYPE =>
            val uriString = list.get(1).toString
            try {
              val uri = new URI(uriString)
              if (uri.getScheme == "twtr" && uri.getSchemeSpecificPart != null) {
                Some(uriString) // Returns the full URI: twtr:svc:...
              } else None
            } catch {
              case NonFatal(_) => None
            }
        }.flatten
      }

      serviceIdFromSan match {
        case Some(id) => Some(id)
        case None =>
          // Fallback to Common Name (CN)
          val dn = cert.getSubjectX500Principal.getName(X500Principal.RFC2253)
          val cn = dn
            .split(',')
            .find(_.trim.startsWith("CN="))
            .map(_.trim.substring(3))

          cn.flatMap {
              case id if id.startsWith("twtr:svc:") => Some(id) // Already has prefix
              case ServiceIdentifierRegex(id) => Some(s"twtr:svc:$id") // Add prefix
              case other => Some(s"non_mtls:${other.take(100)}")
            }.orElse(Some(s"non_mtls:${dn.take(100)}"))
      }
    } catch {
      case NonFatal(_) => Some("extraction_failed")
    }
  }
}
