package com.twitter.finagle

import com.twitter.finagle.util.InetSocketAddressUtil
import com.twitter.util.{Future, Duration}
import java.net.SocketAddress

private[finagle]  // for now
trait ListeningServer {
  def close(grace: Duration): Future[Unit]
  def boundAddress: SocketAddress
}

private[finagle]  // for now
trait Server[Req, Rep] {
  def serve(addr: SocketAddress, service: ServiceFactory[Req, Rep]): ListeningServer

  def serve(addr: SocketAddress, service: Service[Req, Rep]): ListeningServer =
    serve(addr, ServiceFactory.const(service))

  def serve(target: String, service: ServiceFactory[Req, Rep]): ListeningServer = {
    val Seq(addr) = InetSocketAddressUtil.parseHosts(target)
    serve(addr, service)
  }

  def serve(target: String, service: Service[Req, Rep]): ListeningServer = {
    serve(target, ServiceFactory.const(service))
  }
}
