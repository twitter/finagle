package com.twitter.finagle.client

import com.twitter.finagle.dispatch.SerialClientDispatcher
import com.twitter.finagle.netty3.Netty3Transporter
import com.twitter.finagle.param.ProtocolLibrary
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.{Name, Service, ServiceFactory, Stack}
import com.twitter.util.Future
import java.net.SocketAddress
import java.nio.charset.StandardCharsets.UTF_8
import org.jboss.netty.channel.{ChannelHandlerContext, ChannelPipelineFactory, Channels, MessageEvent, SimpleChannelHandler}
import org.jboss.netty.handler.codec.string.{StringDecoder, StringEncoder}

private class DelimEncoder(delim: Char) extends SimpleChannelHandler {
  override def writeRequested(ctx: ChannelHandlerContext, evt: MessageEvent) = {
    val newMessage = evt.getMessage match {
      case m: String => m + delim
      case m => m
    }

    Channels.write(ctx, evt.getFuture, newMessage, evt.getRemoteAddress)
  }
}

private[finagle] object StringClientPipeline extends ChannelPipelineFactory {
  def getPipeline = {
    val pipeline = Channels.pipeline()
    pipeline.addLast("stringEncode", new StringEncoder(UTF_8))
    pipeline.addLast("stringDecode", new StringDecoder(UTF_8))
    pipeline.addLast("line", new DelimEncoder('\n'))
    pipeline
  }
}

private[finagle] object NoDelimStringPipeline extends ChannelPipelineFactory {
  def getPipeline = {
    val pipeline = Channels.pipeline()
    pipeline.addLast("stringEncode", new StringEncoder(UTF_8))
    pipeline.addLast("stringDecode", new StringDecoder(UTF_8))
    pipeline
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
      appendDelimeter: Boolean = true)
    extends StdStackClient[String, String, Client]
    with StringRichClient {
    protected def copy1(
      stack: Stack[ServiceFactory[String, String]] = this.stack,
      params: Stack.Params = this.params
    ): Client = copy(stack, params)

    protected type In = String
    protected type Out = String

    protected def newTransporter(addr: SocketAddress): Transporter[String, String] =
      if (appendDelimeter) Netty3Transporter(StringClientPipeline, addr, params)
      else Netty3Transporter(NoDelimStringPipeline, addr, params)

    protected def newDispatcher(
      transport: Transport[In, Out]
    ): Service[String, String] =
      new SerialClientDispatcher(transport)
  }

  val stringClient = Client()
}
