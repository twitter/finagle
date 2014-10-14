package com.twitter.finagle.mdns

import com.twitter.finagle.{Announcement, Announcer, Addr, Resolver}
import com.twitter.util.{Closable, Future, Promise, Return, Throw, Time, Try, Var}
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

private object DNSSD {
  lazy val instance = new DNSSD

  def check() {
    // Throws ClassNotFoundException when
    // DNSSD is not present
    instance
  }

  def resolve(regType: String, domain: String): Var[Addr] = {
    val services = new mutable.HashMap[String, MdnsRecord]()
    val v = Var[Addr](Addr.Pending)

    def mkRecord(args: Array[Object]) = {
      assert(args.size > 5)
      Record(
        flags = args(1).asInstanceOf[Int],
        ifIndex = args(2).asInstanceOf[Int],
        serviceName = args(3).asInstanceOf[String],
        regType = args(4).asInstanceOf[String],
        domain = args(5).asInstanceOf[String])
    }

    val proxy = instance.newProxy(instance.BrowseListenerClass) {
      case ("serviceFound", args)  =>
        val record = mkRecord(args)
        instance.resolve(record) foreach { resolved =>
          val mdnsRecord = MdnsRecord(
            record.serviceName,
            record.regType,
            record.domain,
            new InetSocketAddress(resolved.hostName, resolved.port))

          synchronized {
            services.put(record.serviceName, mdnsRecord)
            v() = Addr.Bound(services.values.toSet: Set[SocketAddress])
          }
        }

      case ("serviceLost", args) =>
        val record = mkRecord(args)
        synchronized {
          if (services.remove(record.serviceName).isDefined)
            v() = Addr.Bound(services.values.toSet: Set[SocketAddress])
        }
    }

    instance.browseMethod.invoke(instance.DNSSDClass,
      instance.UNIQUE.asInstanceOf[Object],
      instance.LOCALHOST_ONLY.asInstanceOf[Object],
      regType.asInstanceOf[String], domain.asInstanceOf[String], proxy)

    v
  }
}

private class DNSSDAnnouncer extends MDNSAnnouncerIface {
  DNSSD.check()

  private[this] val dnssd = DNSSD.instance

  def announce(addr: InetSocketAddress, name: String, regType: String, domain: String) =
    dnssd.register(name, regType, domain, addr.getHostName, addr.getPort)
}

private class DNSSDResolver extends MDNSResolverIface {
  DNSSD.check()

  def resolve(regType: String, domain: String) =
    DNSSD.resolve(regType, domain)
}
