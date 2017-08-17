package com.twitter.finagle.client

import com.twitter.finagle.dispatch.SerialClientDispatcher
import com.twitter.finagle.netty4.Netty4Transporter
import com.twitter.finagle.param.ProtocolLibrary
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.{Name, Service, ServiceFactory, Stack}
import com.twitter.util.Future
import io.netty.channel.{
  ChannelHandlerContext,
  ChannelOutboundHandlerAdapter,
  ChannelPipeline,
  ChannelPromise
}
import io.netty.handler.codec.string.{StringDecoder, StringEncoder}
import java.net.SocketAddress
import java.nio.charset.StandardCharsets.UTF_8

private class DelimEncoder(delim: Char) extends ChannelOutboundHandlerAdapter {
  override def write(ctx: ChannelHandlerContext, msg: Any, p: ChannelPromise): Unit = {
    val delimMsg = msg match {
      case m: String => m + delim
      case m => m
    }
    ctx.write(delimMsg, p)
  }
}

private[finagle] object StringClientPipeline extends (ChannelPipeline => Unit) {
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

private[finagle] object StringClient {
  val protocolLibrary = "string"
}

trait StringClient {
  import StringClient._

  case class RichClient(underlying: Service[String, String]) {
    def ping(): Future[String] = underlying("ping")
  }

  trait StringRichClient { self: com.twitter.finagle.Client[String, String] =>
    def newRichClient(dest: Name, label: String): RichClient =
      RichClient(newService(dest, label))
  }

  case class Client(
    stack: Stack[ServiceFactory[String, String]] = StackClient.newStack,
    params: Stack.Params = Stack.Params.empty + ProtocolLibrary(protocolLibrary),
    appendDelimeter: Boolean = true
  ) extends StdStackClient[String, String, Client]
      with StringRichClient {
    protected def copy1(
      stack: Stack[ServiceFactory[String, String]] = this.stack,
      params: Stack.Params = this.params
    ): Client = copy(stack, params)

    protected type In = String
    protected type Out = String

    protected def newTransporter(addr: SocketAddress): Transporter[String, String] =
      if (appendDelimeter) Netty4Transporter.raw(StringClientPipeline, addr, params)
      else Netty4Transporter.raw(NoDelimStringPipeline, addr, params)

    protected def newDispatcher(
      transport: Transport[In, Out]
    ): Service[String, String] =
      new SerialClientDispatcher(transport)

    def withEndpoint(s: Service[String, String]): Client =
      withStack(
        stack.replace(
          StackClient.Role.prepConn,
          (_: ServiceFactory[String, String]) => ServiceFactory.const(s)
        )
      )
  }

  val stringClient = Client()
}
