package com.twitter.finagle.mdns

import com.twitter.finagle.{Announcement, Announcer, Group, Resolver}
import com.twitter.util.{Closable, Future, Promise, Return, Throw, Time, Try}
import java.lang.reflect.{InvocationHandler, Method, Proxy}
import java.net.{InetSocketAddress, SocketAddress}
import scala.collection.mutable

private case class Record(
  flags: Int,
  ifIndex: Int,
  serviceName: String,
  regType: String,
  domain: String)

private case class ResolvedRecord(
  flags: Int,
  ifIndex: Int,
  fullName: String,
  hostName: String,
  port: Int)

private class Listener(
    f: PartialFunction[(String, Array[Object]), Unit])
  extends InvocationHandler
{
  def invoke(proxy: Object, method: Method, args: Array[Object]) = {
    val fArgs = (method.getName, args)
    if (f.isDefinedAt(fArgs)) f(fArgs)
    null
  }
}

private class DNSSD {
  def newProxy[T](klass: Class[_])(f: PartialFunction[(String, Array[Object]), Unit]) =
    Proxy.newProxyInstance(klass.getClassLoader, Array(klass), new Listener(f))

  def classNamed(name: String): Class[_] =
    Class.forName("com.apple.dnssd." + name)

  val DNSSDClass = classNamed("DNSSD")
  val LOCALHOST_ONLY = DNSSDClass.getField("LOCALHOST_ONLY").getInt(DNSSDClass)
  val UNIQUE = DNSSDClass.getField("UNIQUE").getInt(DNSSDClass)

  val TXTRecordClass = classNamed("TXTRecord")
  val BlankTXTRecord = TXTRecordClass.newInstance.asInstanceOf[Object]

  val RegistrationClass = classNamed("DNSSDRegistration")
  val registrationStopMethod = RegistrationClass.getMethod("stop")

  val RegisterListenerClass = classNamed("RegisterListener")
  val registerMethod = DNSSDClass.getDeclaredMethod(
    "register", classOf[Int], classOf[Int], classOf[String],
    classOf[String], classOf[String], classOf[String], classOf[Int],
    TXTRecordClass, RegisterListenerClass)

  val ResolveListenerClass = classNamed("ResolveListener")
  val resolveMethod = DNSSDClass.getMethod(
    "resolve", classOf[Int], classOf[Int], classOf[String],
    classOf[String], classOf[String], ResolveListenerClass)

  val BrowseListenerClass = classNamed("BrowseListener")
  val browseMethod = DNSSDClass.getMethod(
    "browse", classOf[Int], classOf[Int], classOf[String],
    classOf[String], BrowseListenerClass)

  def register(
    serviceName: String,
    regType: String,
    domain: String,
    host: String,
    port: Int
  ): Future[Announcement] = {
    val reply = new Promise[Announcement]

    val proxy = newProxy(RegisterListenerClass) {
      case ("serviceRegistered", args) =>
        val announcement = new Announcement {
          def unannounce() = {
            registrationStopMethod.invoke(args(0))
            Future.Done
          }
        }
        reply.setValue(announcement)

      case ("operationFailed", _) =>
        reply.setException(new Exception("Registration failed"))
    }

    registerMethod.invoke(DNSSDClass,
      UNIQUE.asInstanceOf[Object], LOCALHOST_ONLY.asInstanceOf[Object],
      serviceName, regType, domain, host, port.asInstanceOf[Object], BlankTXTRecord, proxy)

    reply
  }

  def resolve(record: Record): Future[ResolvedRecord] = {
    val reply = new Promise[ResolvedRecord]
    val proxy = newProxy(ResolveListenerClass) {
      case ("serviceResolved", args) =>
        reply.setValue(ResolvedRecord(
          flags = args(1).asInstanceOf[Int],
          ifIndex = args(2).asInstanceOf[Int],
          fullName = args(3).asInstanceOf[String],
          hostName = args(4).asInstanceOf[String],
          port = args(5).asInstanceOf[Int]))

      case ("operationFailed", _) =>
        reply.setException(new Exception("Resolve failed"))
    }

    resolveMethod.invoke(
      DNSSDClass, record.flags.asInstanceOf[Object], record.ifIndex.asInstanceOf[Object],
      record.serviceName, record.regType, record.domain, proxy)

    reply
  }
}

private[mdns] class DNSSDGroup(dnssd: DNSSD, regType: String, domain: String)
  extends Group[SocketAddress]
{
  private[this] val services = new mutable.HashMap[String, SocketAddress]()
  @volatile private[this] var current = Set[SocketAddress]()

  def members = current

  private[this] def mkRecord(args: Array[Object]) =
    Record(
      flags = args(1).asInstanceOf[Int],
      ifIndex = args(2).asInstanceOf[Int],
      serviceName = args(3).asInstanceOf[String],
      regType = args(4).asInstanceOf[String],
      domain = args(5).asInstanceOf[String])

  private[this] val proxy = dnssd.newProxy(dnssd.BrowseListenerClass) {
    case ("serviceFound", args)  =>
      val record = mkRecord(args)
      dnssd.resolve(record) foreach { resolved=>
        synchronized {
          services.put(record.serviceName, new InetSocketAddress(resolved.hostName, resolved.port))
          current = services.values.toSet
        }
      }

    case ("serviceLost", args) =>
      val record = mkRecord(args)
      synchronized {
        if (services.remove(record.serviceName).isDefined)
          current = services.values.toSet
      }
  }

  dnssd.browseMethod.invoke(dnssd.DNSSDClass,
    dnssd.UNIQUE.asInstanceOf[Object], dnssd.LOCALHOST_ONLY.asInstanceOf[Object],
    regType.asInstanceOf[String], domain.asInstanceOf[String], proxy)
}

private[mdns] class DNSSDAnnouncer extends Announcer {
  val scheme = "dnssd"

  private[this] val dnssd = new DNSSD

  def announce(addr: InetSocketAddress, target: String): Future[Announcement] = {
    val Array(name, app, prot, domain) = target.split("\\.")
    dnssd.register(name, app + "." + prot, domain, addr.getHostName, addr.getPort)
  }
}

private[mdns] class DNSSDResolver extends Resolver {
  val scheme = "dnssd"

  private[this] val dnssd = new DNSSD

  def resolve(target: String): Try[Group[SocketAddress]] = {
    val Array(app, prot, domain) = target.split("\\.")
    Return(new DNSSDGroup(dnssd, app + "." + prot, domain))
  }
}
