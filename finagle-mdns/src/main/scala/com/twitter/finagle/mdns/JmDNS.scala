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

private[mdns] class JmDNSAnnouncer extends Announcer {
  val scheme = "jmdns"

  def announce(addr: InetSocketAddress, target: String): Future[Announcement] = {
    val Array(name, app, prot, domain) = target.split("\\.")
    val regType = app + "." + prot + "." + domain + "."
    val info = ServiceInfo.create(regType, name, addr.getPort, "")

    DNS.registerService(info) map { _ =>
      new Announcement {
        def unannounce() = DNS.unregisterService(info) map { _ => () }
      }
    }
  }
}

private[mdns] class JmDNSResolver extends Resolver {
  val scheme = "jmdns"

  def resolve(target: String): Try[Group[SocketAddress]] = {
    val Array(app, prot, domain) = target.split("\\.")
    val group = new JmDNSGroup(app + "." + prot + "." + domain + ".")
    Return(group)
  }
}

private[mdns] class JmDNSGroup(regType: String) extends Group[SocketAddress] {
  private[this] val services = new mutable.HashMap[String, SocketAddress]()
  @volatile private[this] var current = Set[SocketAddress]()
  def members = current

  DNS.addServiceListener(regType, new ServiceListener {
    def serviceResolved(event: ServiceEvent) {}

    def serviceAdded(event: ServiceEvent) {
      DNS.getServiceInfo(event.getType, event.getName) foreach { info =>
        val addresses = info.getInetAddresses
        val socket: SocketAddress = new InetSocketAddress(addresses(0), info.getPort)
        synchronized {
          services.put(info.getName, socket)
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
