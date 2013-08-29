package com.twitter.finagle

import com.twitter.finagle.stats.{ClientStatsReceiver, StatsReceiver}
import com.twitter.finagle.thrift.ThriftClientRequest
import com.twitter.util.Future
import java.lang.reflect.Constructor
import java.net.SocketAddress
import org.apache.thrift.protocol.TProtocolFactory
import org.jboss.netty.buffer.ChannelBuffer

private object ThriftUtil {
  def findClass[A](name: String): Option[Class[A]] =
    try {
      Some(Class.forName(name).asInstanceOf[Class[A]])
    } catch {
      case _: ClassNotFoundException => None
    }

  def findConstructor[A](clz: Class[A], paramTypes: Class[_]*): Option[Constructor[A]] =
    try {
      Some(clz.getConstructor(paramTypes: _*))
    } catch {
      case _: NoSuchMethodException => None
    }

  def findRootWithSuffix(str: String, suffix: String): Option[String] =
    if (str.endsWith(suffix)) Some(str.dropRight(suffix.length)) else None
}

/**
 * A mixin trait to provide a rich Thrift client API.
 *
 * @define clientExampleObject ThriftRichClient
 *
 * @define clientExample
 *
 * For example, this IDL:
 *
 * {{{
 * service TestService {
 *   string query(1: string x)
 * }
 * }}}
 *
 * compiled with Scrooge, generates the interface
 * `TestService.FutureIface`. This is then passed
 * into `newIface`:
 *
 * {{{
 * $clientExampleObject.newIface[TestService.FutureIface](
 *   addr, classOf[TestService.FutureIface])
 * }}}
 *
 * However note that the Scala compiler can insert the latter
 * `Class` for us, for which another variant of `newIface` is
 * provided:
 *
 * {{{
 * $clientExampleObject.newIface[TestService.FutureIface](addr)
 * }}}
 *
 * In Java, we need to provide the class object:
 *
 * {{{
 * TestService.FutureIface client =
 *   $clientExampleObject.newIface(addr, TestService.FutureIface.class);
 * }}}
 *
 * @define clientUse
 *
 * Create a new client of type `Iface`, which must be generated
 * by either [[https://github.com/twitter/scrooge Scrooge]] or
 * [[https://github.com/mariusaeriksen/thrift-0.5.0-finagle thrift-finagle]].
 *
 * @define thriftUpgrade
 *
 * The client uses the standard thrift protocols, with support for
 * both framed and buffered transports. Finagle attempts to upgrade
 * the protocol in order to ship an extra envelope carrying trace IDs
 * and client IDs associated with the request. These are used by
 * Finagle's tracing facilities and may be collected via aggregators
 * like [[http://twitter.github.com/zipkin/ Zipkin]].
 *
 * The negotiation is simple: on connection establishment, an
 * improbably-named method is dispatched on the server. If that
 * method isn't found, we are dealing with a legacy thrift server,
 * and the standard protocol is used. If the remote server is also a
 * finagle server (or any other supporting this extension), we reply
 * to the request, and every subsequent request is dispatched with an
 * envelope carrying trace metadata. The envelope itself is also a
 * Thrift struct described [[https://github.com/twitter/finagle/blob/master/finagle-thrift/src/main/thrift/tracing.thrift here]].
 */
trait ThriftRichClient { self: Client[ThriftClientRequest, Array[Byte]] =>
  import ThriftUtil._

  protected val protocolFactory: TProtocolFactory
  /** The client name used when group isn't named. */
  protected val defaultClientName: String

  private val thriftFinagleClientParamTypes =
    Seq(classOf[Service[_, _]], classOf[TProtocolFactory])

  private val scrooge2FinagleClientParamTypes =
    Seq(
      classOf[Service[_, _]],
      classOf[TProtocolFactory],
      classOf[Option[_]],
      classOf[StatsReceiver])

  private val scrooge3FinagleClientParamTypes =
    Seq(
      classOf[Service[_, _]],
      classOf[TProtocolFactory],
      classOf[String],
      classOf[StatsReceiver])

  /**
   * $clientUse
   */
  def newIface[Iface](addr: String, cls: Class[_]): Iface =
    newIface(Resolver.resolve(addr)(), cls)

  /**
   * $clientUse
   */
  def newIface[Iface: ClassManifest](addr: String): Iface =
    newIface[Iface](Resolver.resolve(addr)())

  /**
   * $clientUse
   */
  def newIface[Iface: ClassManifest](group: Group[SocketAddress]): Iface = {
    val cls = implicitly[ClassManifest[Iface]].erasure
    newIface[Iface](group, cls)
  }

  /**
   * $clientUse
   */
  def newIface[Iface](group: Group[SocketAddress], cls: Class[_]): Iface = {
    val clsName = cls.getName
    lazy val underlying = newClient(group).toService
    lazy val stats =
      group match {
        case NamedGroup(name) => ClientStatsReceiver.scope(name)
        case _ => ClientStatsReceiver.scope(defaultClientName)
      }

    def tryThriftFinagleClient: Option[Iface] =
      for {
        baseName   <- findRootWithSuffix(clsName, "$ServiceIface")
        clientCls  <- findClass[Iface](baseName + "$ServiceToClient")
        cons       <- findConstructor(clientCls, thriftFinagleClientParamTypes: _*)
      } yield cons.newInstance(underlying, protocolFactory)

    def tryScrooge3FinagleClient: Option[Iface] =
      for {
        clientCls  <- findClass[Iface](clsName + "$FinagleClient")
        cons       <- findConstructor(clientCls, scrooge3FinagleClientParamTypes: _*)
      } yield cons.newInstance(underlying, protocolFactory, "", stats)

    def tryScrooge3FinagledClient: Option[Iface] =
      for {
        baseName   <- findRootWithSuffix(clsName, "$FutureIface")
        clientCls  <- findClass[Iface](baseName + "$FinagledClient")
        cons       <- findConstructor(clientCls, scrooge3FinagleClientParamTypes: _*)
      } yield cons.newInstance(underlying, protocolFactory, "", stats)

    def tryScrooge2Client: Option[Iface] =
      for {
        baseName   <- findRootWithSuffix(clsName, "$FutureIface")
        clientCls  <- findClass[Iface](baseName + "$FinagledClient")
        cons       <- findConstructor(clientCls, scrooge2FinagleClientParamTypes: _*)
      } yield cons.newInstance(underlying, protocolFactory, None, stats)

    val iface =
      tryThriftFinagleClient orElse
      tryScrooge3FinagleClient orElse
      tryScrooge3FinagledClient orElse
      tryScrooge2Client

    iface getOrElse {
      throw new IllegalArgumentException("Iface %s is not a valid thrift iface".format(clsName))
    }
  }
}

/**
 * A mixin trait to provide a rich Thrift server API.
 *
 * @define serverExample
 *
 * `TestService.FutureIface` must be implemented and passed
 * into `serveIface`:
 *
 * {{{
 * $serverExampleObject.serveIface(":*", new TestService.FutureIface {
 *   def query(x: String) = Future.value(x)  // (echo service)
 * })
 * }}}
 *
 * @define serverExampleObject ThriftMuxRichServer
 */
trait ThriftRichServer { self: Server[Array[Byte], Array[Byte]] =>
  import ThriftUtil._

  private type BinaryService = Service[Array[Byte], Array[Byte]]

  protected val protocolFactory: TProtocolFactory

  private[this] def serverFromIface(impl: AnyRef): BinaryService = {
    def tryThriftFinagleService(iface: Class[_]): Option[BinaryService] =
      for {
        baseName   <- findRootWithSuffix(iface.getName, "$ServiceIface")
        serviceCls <- findClass[BinaryService](baseName + "$Service")
        cons       <- findConstructor(serviceCls, iface, classOf[TProtocolFactory])
      } yield cons.newInstance(impl, protocolFactory)

    def tryScroogeFinagledService(iface: Class[_]): Option[BinaryService] =
      for {
        baseName   <- findRootWithSuffix(iface.getName, "$FutureIface") orElse
                      Some(iface.getName)
        serviceCls <- findClass[BinaryService](baseName + "$FinagleService") orElse
                      findClass[BinaryService](baseName + "$FinagledService")
        cons       <- findConstructor(serviceCls, iface, classOf[TProtocolFactory])
      } yield cons.newInstance(impl, protocolFactory)

    def tryClass(cls: Class[_]): Option[BinaryService] =
      tryThriftFinagleService(cls) orElse tryScroogeFinagledService(cls) orElse
        // walk the entire inheritance graph
        (Option(cls.getSuperclass) ++ cls.getInterfaces).view.flatMap(tryClass).headOption

    tryClass(impl.getClass).getOrElse {
      throw new IllegalArgumentException("argument implements no candidate ifaces")
    }
  }

  /**
   * @define serveIface
   *
   * Serve the interface implementation `iface`, which must be generated
   * by either [[https://github.com/twitter/scrooge Scrooge]] or
   * [[https://github.com/mariusaeriksen/thrift-0.5.0-finagle thrift-finagle]].
   *
   * Given the IDL:
   *
   * {{{
   * service TestService {
   *   string query(1: string x)
   * }
   * }}}
   *
   * Scrooge will generate an interface, `TestService.FutureIface`,
   * implementing the above IDL.
   *
   * $serverExample
   *
   * Note that this interface is discovered by reflection. Passing an
   * invalid interface implementation will result in a runtime error.
   */
  def serveIface(addr: String, iface: AnyRef): ListeningServer =
    serve(addr, serverFromIface(iface))

  /** $serveIface */
  def serveIface(addr: SocketAddress, iface: AnyRef): ListeningServer =
    serve(addr, serverFromIface(iface))
}
