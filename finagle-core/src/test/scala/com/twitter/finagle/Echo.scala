package com.twitter.finagle

import com.twitter.finagle.client.StringClient
import com.twitter.finagle.server.StringServer
import java.net.SocketAddress

private[finagle] object Echo
  extends Client[String, String] with StringClient
  with Server[String, String] with StringServer {

  def serve(addr: SocketAddress, service: ServiceFactory[String, String]) =
    stringServer.serve(addr, service)

  def newClient(dest: Name, label: String) =
    stringClient.newClient(dest, label)

  def newService(dest: Name, label: String) =
    stringClient.newService(dest, label)

}
