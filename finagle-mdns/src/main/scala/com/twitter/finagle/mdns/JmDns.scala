package com.twitter.finagle.mdns

import com.twitter.finagle.{Announcement, Addr, Address}
import com.twitter.util.{Future, FuturePool, Var}
import java.net.InetSocketAddress
import javax.jmdns._
import scala.collection.mutable

/**
 * This is used implicitly by the `mdns` announcer/resolver and is exposed
 * since it may be useful to manage the resources explicitly along with
 * a processes life-cycle.
 */
object JmDnsInstance {
  private[this] val dns = JmDNS.create(null, null)
  private[this] val pool = FuturePool.unboundedPool

  def registerService(info: ServiceInfo): Future[Unit] =
    pool { dns.registerService(info) }

  def unregisterService(info: ServiceInfo): Future[Unit] =
    pool { dns.unregisterService(info) }

  def addServiceListener(regType: String, listener: ServiceListener): Unit =
    dns.addServiceListener(regType, listener)

  def getServiceInfo(regType: String, name: String): Future[ServiceInfo] =
    pool { dns.getServiceInfo(regType, name) }

  def close(): Future[Unit] = pool { dns.close() }
}

private class JmDnsAnnouncer {
  def announce(
    addr: InetSocketAddress,
    name: String,
    regType: String,
    domain: String
  ): Future[Announcement] = {
    val info = ServiceInfo.create(s"$regType.$domain", name, addr.getPort, "")
    JmDnsInstance.registerService(info).map { _ =>
      new Announcement {
        def unannounce(): Future[Unit] = JmDnsInstance.unregisterService(info).unit
      }
    }
  }
}

private class JmDnsResolver {
  def resolve(regType: String, domain: String): Var[Addr] = {
    val services = new mutable.HashMap[String, Address]()
    val v = Var[Addr](Addr.Pending)
    JmDnsInstance.addServiceListener(s"$regType.$domain.", new ServiceListener {
      def serviceResolved(event: ServiceEvent): Unit = {}

      def serviceAdded(event: ServiceEvent): Unit = {
        JmDnsInstance.getServiceInfo(event.getType, event.getName).foreach { info =>
          val addresses = info.getInetAddresses
          val metadata = MdnsAddrMetadata(
            info.getName,
            s"${info.getApplication}.${info.getProtocol}",
            info.getDomain)
          val addr = Address.Inet(
            new InetSocketAddress(addresses(0), info.getPort),
            MdnsAddrMetadata.toAddrMetadata(metadata))

          synchronized {
            services.put(info.getName, addr)
            v() = Addr.Bound(services.values.toSet: Set[Address])
          }
        }
      }

      def serviceRemoved(event: ServiceEvent): Unit = {
        synchronized {
          if (services.remove(event.getName).isDefined)
            v() = Addr.Bound(services.values.toSet: Set[Address])
        }
      }
    })
    v
  }
}