package com.twitter.finagle

import com.twitter.finagle.stats.{DefaultStatsReceiver, StatsReceiver}
import com.twitter.finagle.thrift.ThriftClientRequest
import com.twitter.util.Future
import java.net.SocketAddress
import org.apache.thrift.protocol.{TProtocolFactory, TBinaryProtocol}
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}


/**
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
 * ThriftMux.newIface[TestService.FutureIface](
 *   target, classOf[TestService.FutureIface])
 * }}}
 *
 * However note that the Scala compiler can insert the latter
 * `Class` for us, for which another variant of `newIface` is
 * provided:
 *
 * {{{
 * ThriftMux.newIface[TestService.FutureIface](target)
 * }}}
 *
 * In Java, we need to provide the class object:
 *
 * {{{
 * TestService.FutureIface client = 
 *   ThriftMux.newIface(target, TestService.FutureIface.class);
 * }}}
 *
 * @define clientUse
 *   
 * Create a new client of type `Iface`, which must be generated
 * by either [[https://github.com/twitter/scrooge Scrooge]] or
 * [[https://github.com/mariusaeriksen/thrift-0.5.0-finagle thrift-finagle]].
 *
 * $clientExample
*/
trait ThriftMuxRichClient { self: Client[ThriftClientRequest, Array[Byte]] =>
  import ThriftMuxUtil._

  protected val protocolFactory: TProtocolFactory

  /**
   * $clientUse
   */
  def newIface[Iface](target: String, cls: Class[_]): Iface =
    newIface(Group.resolve(target), cls)

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

      val constructor = try { clientClass.getConstructor(
        classOf[Service[_, _]], classOf[TProtocolFactory], 
        classOf[Option[_]], classOf[StatsReceiver])
      } catch {
        case cause: NoSuchMethodException =>
          throw new IllegalArgumentException("Iface is not a valid scrooge iface", cause)
      }
  
      val underlying = newClient(group).toService
      constructor.newInstance(
        underlying, protocolFactory, 
        None, DefaultStatsReceiver)
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
    newIface[Iface](Group.resolve(target))

  /**
   * $clientUse
   */
  def newIface[Iface: ClassManifest](group: Group[SocketAddress]): Iface = {
    val cls = implicitly[ClassManifest[Iface]].erasure  
    newIface[Iface](group, cls)
  }
}

case class ThriftMuxClientImpl(
  muxer: Client[ChannelBuffer, ChannelBuffer],
  protocolFactory: TProtocolFactory = new TBinaryProtocol.Factory()
) extends Client[ThriftClientRequest, Array[Byte]] with ThriftMuxRichClient {
  def newClient(group: Group[SocketAddress]): ServiceFactory[ThriftClientRequest, Array[Byte]] =
    muxer.newClient(group) map { service =>
      new Service[ThriftClientRequest, Array[Byte]] {
        def apply(req: ThriftClientRequest): Future[Array[Byte]] = {
          if (req.oneway) return Future.exception(
            new Exception("ThriftMux does not support one-way messages"))
          
          service(ChannelBuffers.wrappedBuffer(req.message)) map(ThriftMuxUtil.bufferToArray)
        }
        override def isAvailable = service.isAvailable
      }
    }
}

/**
 * A client for thrift served over [[com.twitter.finagle.mux]]
 */
object ThriftMuxClient extends ThriftMuxClientImpl(MuxClient)
