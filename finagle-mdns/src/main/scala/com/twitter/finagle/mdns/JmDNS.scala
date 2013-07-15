package com.twitter.finagle.mdns

import com.twitter.finagle.{Announcer, Announcement, Group, Resolver}
import com.twitter.util.{Future, FuturePool, Return, Throw, Try}
import java.net.{InetSocketAddress, SocketAddress}
import javax.jmdns._
import scala.collection.mutable

private object DNS {
  val dns = JmDNS.create(null, null)

  private[this] val pool = FuturePool.unboundedPool

  def registerService(info: ServiceInfo) =
    pool { dns.registerService(info) }

  def unregisterService(info: ServiceInfo) =
    pool { dns.unregisterService(info) }

  def addServiceListener(regType: String, listener: ServiceListener) =
    dns.addServiceListener(regType, listener)

  def getServiceInfo(regType: String, name: String) =
    pool { dns.getServiceInfo(regType, name) }
}

private class JmDNSAnnouncer extends MDNSAnnouncerIface {
  val scheme = "jmdns"

  def announce(
    addr: InetSocketAddress,
    name: String,
    regType: String,
    domain: String
  ): Future[Announcement] = {
    val info = ServiceInfo.create(regType + "." + domain, name, addr.getPort, "")
    DNS.registerService(info) map { _ =>
      new Announcement {
        def unannounce() = DNS.unregisterService(info) map { _ => () }
      }
    }
  }
}

private class JmDNSResolver extends MDNSResolverIface {
  val scheme = "jmdns"

  def resolve(regType: String, domain: String): Try[Group[MdnsRecord]] =
    Return(new JmDNSGroup(regType + "." + domain + "."))
}

private class JmDNSGroup(regType: String) extends Group[MdnsRecord] {
  private[this] val services = new mutable.HashMap[String, MdnsRecord]()
  @volatile private[this] var current = Set[MdnsRecord]()
  def members = current

  DNS.addServiceListener(regType, new ServiceListener {
    def serviceResolved(event: ServiceEvent) {}

    def serviceAdded(event: ServiceEvent) {
      DNS.getServiceInfo(event.getType, event.getName) foreach { info =>
        val addresses = info.getInetAddresses
        val mdnsRecord = MdnsRecord(
          info.getName,
          info.getApplication + "." + info.getProtocol,
          info.getDomain,
          new InetSocketAddress(addresses(0), info.getPort))

        synchronized {
          services.put(info.getName, mdnsRecord)
          current = services.values.toSet
        }
      }
    }

    def serviceRemoved(event: ServiceEvent) {
      synchronized {
        if (services.remove(event.getName).isDefined)
          current = services.values.toSet
      }
    }
  })
}
