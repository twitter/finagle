package com.twitter.finagle.client.utils

import com.twitter.finagle.client.{StackClient, StdStackClient, Transporter}
import com.twitter.finagle.dispatch.SerialClientDispatcher
import com.twitter.finagle.netty4.Netty4Transporter
import com.twitter.finagle.param.ProtocolLibrary
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.transport.{Transport, TransportContext}
import com.twitter.finagle.{Service, ServiceFactory, Stack}
import io.netty.channel.{
  ChannelHandlerContext,
  ChannelOutboundHandlerAdapter,
  ChannelPipeline,
  ChannelPromise
}
import io.netty.handler.codec.string.{StringDecoder, StringEncoder}
import java.net.SocketAddress
import java.nio.charset.StandardCharsets.UTF_8

object StringClient {

  val protocolLibrary = "string"

  object StringClientPipeline extends (ChannelPipeline => Unit) {

    private class DelimEncoder(delim: Char) extends ChannelOutboundHandlerAdapter {
      override def write(ctx: ChannelHandlerContext, msg: Any, p: ChannelPromise): Unit = {
        val delimMsg = msg match {
          case m: String => m + delim
          case m => m
        }
        ctx.write(delimMsg, p)
      }
    }

    def apply(pipeline: ChannelPipeline): Unit = {
      pipeline.addLast("stringEncode", new StringEncoder(UTF_8))
      pipeline.addLast("stringDecode", new StringDecoder(UTF_8))
      pipeline.addLast("line", new DelimEncoder('\n'))
    }
  }

  private[finagle] object NoDelimStringPipeline extends (ChannelPipeline => Unit) {
    def apply(pipeline: ChannelPipeline): Unit = {
      pipeline.addLast("stringEncode", new StringEncoder(UTF_8))
      pipeline.addLast("stringDecode", new StringDecoder(UTF_8))
    }
  }

  val DefaultParams: Stack.Params = Stack.Params.empty + ProtocolLibrary(protocolLibrary)

  case class Client(
    stack: Stack[ServiceFactory[String, String]] = StackClient.newStack,
    params: Stack.Params = DefaultParams,
    appendDelimiter: Boolean = true)
      extends StdStackClient[String, String, Client]
      with Stack.Transformable[Client] {
    protected def copy1(
      stack: Stack[ServiceFactory[String, String]] = this.stack,
      params: Stack.Params = this.params
    ): Client = copy(stack, params)

    protected type In = String
    protected type Out = String
    protected type Context = TransportContext

    protected def newTransporter(
      addr: SocketAddress
    ): Transporter[String, String, TransportContext] =
      if (appendDelimiter) Netty4Transporter.raw(StringClientPipeline, addr, params)
      else Netty4Transporter.raw(NoDelimStringPipeline, addr, params)

    protected def newDispatcher(
      transport: Transport[In, Out] { type Context <: Client.this.Context }
    ): Service[String, String] =
      new SerialClientDispatcher(transport, NullStatsReceiver)

    def withEndpoint(s: Service[String, String]): Client =
      withStack(
        stack.replace(
          StackClient.Role.prepConn,
          (_: ServiceFactory[String, String]) => ServiceFactory.const(s)
        )
      )

    override def transformed(t: Stack.Transformer): Client =
      withStack(t(stack))

  }

  def client: Client = Client()
}
