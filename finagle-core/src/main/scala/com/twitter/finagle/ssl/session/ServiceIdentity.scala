package com.twitter.finagle.ssl.session

/**
 * Base class representing an authenticated service identity derived from an SslSession.
 * These derived identities can be used for secure access controls, rate limiting, and quota controls.
 */
abstract class ServiceIdentity extends Serializable {
  val name: String
}

object ServiceIdentity {

  /** @see https://datatracker.ietf.org/doc/html/rfc5280#section-4.2.1.6 */
  private[finagle] val UriTag = 6
  private[finagle] val DnsTag = 2
  private[finagle] val IpTag = 7
  private[finagle] case class GeneralName(tag: Int, name: String)
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
    name.tag match {
      case UriTag => new UriServiceIdentity(name.name)
      case DnsTag => new DnsServiceIdentity(name.name)
      case IpTag => new IpServiceIdentity(name.name)
      case _ => new GeneralNameServiceIdentity(name.name)
    }
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
    def findTag(tag: Int): Option[GeneralName] = names.find(_.tag == tag)
    val generalName =
      findTag(UriTag).orElse(findTag(DnsTag).orElse(findTag(IpTag).orElse(names.headOption)))
    generalName.map(ServiceIdentity.apply)
  }
}
