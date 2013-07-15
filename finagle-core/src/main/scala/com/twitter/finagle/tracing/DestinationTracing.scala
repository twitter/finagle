package com.twitter.finagle.tracing
import com.twitter.finagle._
import java.net.{SocketAddress, InetSocketAddress}

class ServerDestTracingProxy[Req, Rep](self: ServiceFactory[Req, Rep])
  extends ServiceFactoryProxy[Req, Rep](self)
{
  override def apply(conn: ClientConnection) = {
    // this filter gymnastics is done so that annotation occurs after
    // traceId is set by any inbound request with tracing enabled
    val filter = new SimpleFilter[Req,Rep] {
      def apply(request: Req, service: Service[Req, Rep]) = {
        if (Trace.isActivelyTracing) {
          conn.localAddress match {
            case ia: InetSocketAddress =>
              Trace.recordLocalAddr(ia)
              Trace.recordServerAddr(ia)
            case _ => // do nothing for non-ip address
          }
          conn.remoteAddress match {
            case ia: InetSocketAddress =>
              Trace.recordClientAddr(ia)
            case _ => // do nothing for non-ip address
          }
        }
        service(request)
      }
    }

    self(conn) map { filter andThen _ }
  }
}

/**
 * Filter for clients to record the remote address of the server.  We don't log
 * the local addr here because it's already done in the Dispatcher
 */
class ClientDestTracingFilter[Req,Rep](remoteSock: SocketAddress)
  extends SimpleFilter[Req,Rep]
{
  def apply(request: Req, service: Service[Req, Rep]) = {
    val ret = service(request)
    remoteSock match {
      case ia: InetSocketAddress =>
        Trace.recordServerAddr(ia)
      case _ => // do nothing for non-ip address
    }
    ret
  }
}


