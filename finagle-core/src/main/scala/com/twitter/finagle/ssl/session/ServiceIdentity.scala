package com.twitter.finagle.ssl.session

import org.bouncycastle.asn1.x509.GeneralName

/**
 * Base class representing an authenticated service identity derived from an SslSession.
 * These derived identities can be used for secure access controls, rate limiting, and quota controls.
 */
abstract class ServiceIdentity extends Serializable {
  val name: String
}

object ServiceIdentity {
  private val UriTag = 6
  private val DnsTag = 2
  private val IpTag = 7
  class DnsServiceIdentity(override val name: String) extends ServiceIdentity

  class IpServiceIdentity(override val name: String) extends ServiceIdentity

  class UriServiceIdentity(override val name: String) extends ServiceIdentity

  class GeneralNameServiceIdentity(override val name: String) extends ServiceIdentity

  /**
   * Extracts a [[ServiceIdentity]] from a [[GeneralName]], typically from the SubjectAlternativeNames x509 extension extracted
   *  from a local or peer cert.
   *
   * @param name the [[GeneralName]]
   * @return a service identity with a corresponding type for common tags (URI, DNS, IP), or a generic
   *         identity for other tag types
   */
  def apply(name: GeneralName): ServiceIdentity = {
    val strName = name.toString.split(": ", 2)(1)
    if (name.getTagNo == UriTag)
      new UriServiceIdentity(strName)
    else if (name.getTagNo == DnsTag)
      new DnsServiceIdentity(strName)
    else if (name.getTagNo == IpTag)
      new IpServiceIdentity(strName)
    else new GeneralNameServiceIdentity(strName)
  }

  /**
   * Extracts a [[ServiceIdentity]] from a seq of [[GeneralName]], typically from the SubjectAlternativeNames x509 extension extracted
   *  from a local or peer cert.
   *
   * @note We prefer specific tag types when extracting. Our order of preference is Uri, Dns, Ip, Any.
   *
   * @param names the seq of [[GeneralName]]
   * @return a service identity with a corresponding type for common tags (URI, DNS, IP), or a generic
   *         identity for other tag types
   */
  def apply(names: Seq[GeneralName]): Option[ServiceIdentity] = {
    val generalName = names
      .find(_.getTagNo == UriTag).orElse(
        names
          .find(_.getTagNo == DnsTag).orElse(
            names.find(_.getTagNo == IpTag).orElse(names.headOption)))
    generalName.map(ServiceIdentity.apply)
  }
}
