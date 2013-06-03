package com.twitter.finagle

import com.twitter.finagle.stats.{ClientStatsReceiver, StatsReceiver}
import com.twitter.finagle.thrift.ThriftClientRequest
import com.twitter.util.Future
import java.net.SocketAddress
import org.apache.thrift.protocol.TProtocolFactory
import org.jboss.netty.buffer.ChannelBuffer

private object ThriftUtil {
  def classForName(name: String) =
    try Class.forName(name) catch {
      case cause: ClassNotFoundException =>
        throw new IllegalArgumentException("Iface is not a valid thrift iface", cause)
    }
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
 *   target, classOf[TestService.FutureIface])
 * }}}
 *
 * However note that the Scala compiler can insert the latter
 * `Class` for us, for which another variant of `newIface` is
 * provided:
 *
 * {{{
 * $clientExampleObject.newIface[TestService.FutureIface](target)
 * }}}
 *
 * In Java, we need to provide the class object:
 *
 * {{{
 * TestService.FutureIface client =
 *   $clientExampleObject.newIface(target, TestService.FutureIface.class);
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

  /**
   * $clientUse
   */
  def newIface[Iface](target: String, cls: Class[_]): Iface =
    newIface(Resolver.resolve(target)(), cls)

  /**
   * $clientUse
   */
  def newIface[Iface](group: Group[SocketAddress], cls: Class[_]): Iface = {
    val clsName = cls.getName
    val inst = if (clsName.endsWith("$ServiceIface")) {  // thrift-finagle
      val clientClass = classForName(clsName.dropRight(13)+"$ServiceToClient")

      val constructor = try { clientClass.getConstructor(
        classOf[Service[_, _]], classOf[TProtocolFactory])
      } catch {
        case cause: NoSuchMethodException =>
          throw new IllegalArgumentException("Iface is not a valid thrift-finagle iface", cause)
      }

      val underlying = newClient(group).toService
      constructor.newInstance(underlying, protocolFactory)
    } else if (clsName.endsWith("$FutureIface")) {  // scrooge
      val clientClass = classForName(clsName.dropRight(12)+"$FinagledClient")

      val statsReceiver = group match {
        case NamedGroup(name) => ClientStatsReceiver.scope(name)
        case _ => ClientStatsReceiver.scope(defaultClientName)
      }

      val underlying = newClient(group).toService

      try {
        // Scrooge 2
        val constructor = clientClass.getConstructor(
          classOf[Service[_, _]], classOf[TProtocolFactory],
          classOf[Option[_]], classOf[StatsReceiver])

        constructor.newInstance(
          underlying, protocolFactory,
          None, statsReceiver)
      } catch {
        case cause: NoSuchMethodException =>
          try {
            // Scrooge 3
            val constructor = clientClass.getConstructor(
              classOf[Service[_, _]], classOf[TProtocolFactory],
              classOf[String], classOf[StatsReceiver])

            constructor.newInstance(
              underlying, protocolFactory,
              "", statsReceiver)
          } catch {
            case cause: NoSuchMethodException =>
              throw new IllegalArgumentException("Iface is not a valid scrooge iface", cause)
          }
      }
    } else {
      throw new IllegalArgumentException(
        "Iface %s is not a valid thrift iface".format(clsName))
    }

    inst.asInstanceOf[Iface]
  }

  /**
   * $clientUse
   */
  def newIface[Iface: ClassManifest](target: String): Iface =
    newIface[Iface](Resolver.resolve(target)())

  /**
   * $clientUse
   */
  def newIface[Iface: ClassManifest](group: Group[SocketAddress]): Iface = {
    val cls = implicitly[ClassManifest[Iface]].erasure
    newIface[Iface](group, cls)
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

  protected val protocolFactory: TProtocolFactory

  private[this] def serverFromIface(iface: AnyRef): Service[Array[Byte], Array[Byte]] = {
    iface.getClass.getInterfaces.filter(n =>
      n.getName.endsWith("$FutureIface") ||
      n.getName.endsWith("$ServiceIface")
    ).toSeq match {
      case Seq(futureIface) if futureIface.getName.endsWith("$FutureIface") =>
        val objectName = futureIface.getName.dropRight(12)
        val serviceClass = classForName(objectName + "$FinagledService")

        val constructor = try {
          serviceClass.getConstructor(futureIface, classOf[TProtocolFactory])
        } catch {
          case cause: NoSuchMethodException =>
            throw new IllegalArgumentException("iface is not a valid FinagledService", cause)
        }

        constructor.newInstance(iface, protocolFactory)
          .asInstanceOf[Service[Array[Byte], Array[Byte]]]

      case Seq(serviceIface) if serviceIface.getName.endsWith("$ServiceIface") =>
        val outerClass = serviceIface.getName.dropRight(13)
        val serviceClass = classForName(outerClass + "$Service")

        val constructor = try {
          serviceClass.getConstructor(serviceIface, classOf[TProtocolFactory])
        } catch {
          case cause: NoSuchMethodException =>
            throw new IllegalArgumentException("iface is not a valid ServiceIface", cause)
        }

        constructor.newInstance(iface, protocolFactory)
          .asInstanceOf[Service[Array[Byte], Array[Byte]]]

      case Seq() => throw new IllegalArgumentException("iface is not a FutureIface or ServiceIface")
      case _ => throw new IllegalArgumentException("iface implements no candidate ifaces")
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
  def serveIface(target: String, iface: AnyRef): ListeningServer =
    serve(target, serverFromIface(iface))

  /** $serveIface */
  def serveIface(target: SocketAddress, iface: AnyRef): ListeningServer =
    serve(target, serverFromIface(iface))
}
