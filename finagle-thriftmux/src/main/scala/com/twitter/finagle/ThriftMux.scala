package com.twitter.finagle

import com.twitter.finagle.client.{RichStackClient, StackClient}
import com.twitter.finagle.thrift.{Protocols, ThriftClientRequest}
import java.net.SocketAddress
import org.jboss.netty.buffer.ChannelBuffer

/**
 * ThriftMux is a client and server for thrift, using
 * [[com.twitter.finagle.mux]] as a transport. Rich interfaces are
 * provided to serve interfaces generated from a
 * [[http://thrift.apache.org/docs/idl/ thrift IDL]] directly from
 * [[https://github.com/twitter/scrooge Scrooge]] or
 * [[https://github.com/mariusaeriksen/thrift-0.5.0-finagle thrift-finagle]].
 *
 * Clients can be created directly from an interface generated via
 * a Thrift IDL:
 *
 * $clientExample
 *
 * Servers are also simple to expose:
 *
 * $serverExample
 *
 * By default, the thrift binary protocol is used; different protocol
 * factories may be supplied by instantiating new clients or servers.
 *
 * @define clientExampleObject ThriftMux
 */
object ThriftMux
  extends Client[ThriftClientRequest, Array[Byte]] with ThriftRichClient
  with Server[Array[Byte], Array[Byte]] with ThriftRichServer
{
  protected val protocolFactory = Protocols.binaryFactory()
  protected val defaultClientName = "mux"

  def newClient(dest: Name, label: String): ServiceFactory[ThriftClientRequest, Array[Byte]] =
    ThriftMuxClient.newClient(dest, label)

  def serve(addr: SocketAddress, service: ServiceFactory[Array[Byte], Array[Byte]]) =
    ThriftMuxServer.serve(addr, service)
}
