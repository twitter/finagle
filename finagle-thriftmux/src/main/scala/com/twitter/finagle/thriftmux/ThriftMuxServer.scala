package com.twitter.finagle

import com.twitter.finagle.thrift.ThriftClientRequest
import com.twitter.util.Future
import java.net.SocketAddress
import org.apache.thrift.protocol.{TProtocolFactory, TBinaryProtocol}
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}

// TODO: factor the *Rich traits into its own package, shared with
// finagle-thrift.

/**
 * @define serverExample
 *
 * `TestService.FutureIface` must be implemented and passed
 * into `serveIface`:
 *
 * {{{
 * ThriftMux.serveIface(":*", new TestService.FutureIface {
 *   def query(x: String) = Future.value(x)  // (echo service)
 * })
 * }}}
 */
trait ThriftMuxRichServer { self: Server[Array[Byte], Array[Byte]] =>
  import ThriftMuxUtil._

  protected val protocolFactory: TProtocolFactory 

  /**
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
  def serveIface(target: String, iface: AnyRef): ListeningServer = {
    val service = iface.getClass.getInterfaces.filter(n => 
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

    serve(target, service)
  }
}

case class ThriftMuxServerImpl(
  muxer: Server[ChannelBuffer, ChannelBuffer],
  protocolFactory: TProtocolFactory = new TBinaryProtocol.Factory()
) extends Server[Array[Byte], Array[Byte]] with ThriftMuxRichServer {
  import ThriftMuxUtil._

  def serve(addr: SocketAddress, newService: ServiceFactory[Array[Byte], Array[Byte]]) =
    muxer.serve(addr, newService map { service =>
      new Service[ChannelBuffer, ChannelBuffer] {
        def apply(req: ChannelBuffer) = {
          val arr = ThriftMuxUtil.bufferToArray(req)
          service(arr) map(ChannelBuffers.wrappedBuffer)
        }
        override def isAvailable = service.isAvailable
      }
    })
}

/**
 * A server for thrift served over [[com.twitter.finagle.mux]].
 */
object ThriftMuxServer extends ThriftMuxServerImpl(MuxServer)
