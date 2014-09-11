package com.twitter.finagle

import com.twitter.finagle.client._
import com.twitter.finagle.netty3._
import com.twitter.finagle.pool.SingletonPool
import com.twitter.finagle.server._
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.transport.Transport
import java.net.SocketAddress
import org.jboss.netty.buffer.{ChannelBuffer => CB}

/**
 * A client and server for the mux protocol described in [[com.twitter.finagle.mux]].
 */
object Mux extends Client[CB, CB] with Server[CB, CB] {

  object Client {
    val stack: Stack[ServiceFactory[CB, CB]] = StackClient.newStack
      .replace(StackClient.Role.pool, SingletonPool.module[CB, CB])
      .replace(StackClient.Role.prepConn, mux.lease.LeasedFactory.module[CB, CB])
  }

  case class Client(
    stack: Stack[ServiceFactory[CB, CB]] = Client.stack,
    params: Stack.Params = StackClient.defaultParams
  ) extends StdStackClient[CB, CB, Client] {
    protected def copy1(
      stack: Stack[ServiceFactory[CB, CB]] = this.stack,
      params: Stack.Params = this.params
    ): Client = copy(stack, params)

    protected type In = CB
    protected type Out = CB

    protected def newTransporter(): Transporter[In, Out] = 
      Netty3Transporter(mux.PipelineFactory, params)
    protected def newDispatcher(transport: Transport[CB, CB]) = {
      val param.Stats(sr) = params[param.Stats]
      new mux.ClientDispatcher(transport, sr)
    }
  }

  val client = Client()
  
  def newClient(dest: Name, label: String): ServiceFactory[CB, CB] = 
    client.newClient(dest, label)
  
  case class Server(
    stack: Stack[ServiceFactory[CB, CB]] = StackServer.newStack,
    params: Stack.Params = StackServer.defaultParams
  ) extends StdStackServer[CB, CB, Server] {
    protected def copy1(
      stack: Stack[ServiceFactory[CB, CB]] = this.stack,
      params: Stack.Params = this.params
    ): Server = copy(stack, params)

    protected type In = CB
    protected type Out = CB

    protected def newListener(): Listener[In, Out] = 
      Netty3Listener(mux.PipelineFactory, params)
    protected def newDispatcher(transport: Transport[In, Out], service: Service[CB, CB]) =
      new mux.ServerDispatcher(transport, service, true, mux.lease.exp.ClockedDrainer.flagged)
  }
  
  val server = Server()

  def serve(addr: SocketAddress, service: ServiceFactory[CB, CB]): ListeningServer =
    server.serve(addr, service)
}
