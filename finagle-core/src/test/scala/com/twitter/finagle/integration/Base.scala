package com.twitter.finagle.integration

import com.twitter.finagle._
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.dispatch.SerialClientDispatcher
import com.twitter.finagle.stats.{InMemoryStatsReceiver, StatsReceiver}
import com.twitter.finagle.tracing.TraceInitializerFilter
import com.twitter.finagle.transport.{ChannelTransport, Transport}
import java.net.SocketAddress
import org.jboss.netty.channel.{Channel, ChannelFactory, ChannelPipeline,
ChannelPipelineFactory, Channels, DefaultChannelConfig}
import org.mockito.Matchers._
import org.mockito.Mockito.when
import org.scalatest.FunSuite
import org.mockito.stubbing.Answer
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito
import org.mockito.invocation.InvocationOnMock

trait IntegrationBase extends FunSuite with MockitoSugar {
  /*
   * Bootstrap enough to get a basic client connection up & running.
   */
  class MockChannel {
    val name = "mock_channel"
    val statsReceiver = new InMemoryStatsReceiver

    val codec = mock[Codec[String, String]]
    when(codec.prepareConnFactory(any[ServiceFactory[String, String]])) thenAnswer {
      new Answer[ServiceFactory[String, String]] {
        def answer(invocation: InvocationOnMock): ServiceFactory[String, String] = {
          val arg = invocation.getArguments.head
          arg.asInstanceOf[ServiceFactory[String, String]]
        }
      }
    }
    when(codec.prepareServiceFactory(any[ServiceFactory[String, String]])) thenAnswer {
      new Answer[ServiceFactory[String, String]] {
        def answer(invocation: InvocationOnMock): ServiceFactory[String, String] = {
          val arg = invocation.getArguments.head
          arg.asInstanceOf[ServiceFactory[String, String]]
        }
      }
    }
    when(codec.newClientTransport(any[Channel], any[StatsReceiver])) thenAnswer {
      new Answer[ChannelTransport[Any, Any]] {
        def answer(invocation: InvocationOnMock): ChannelTransport[Any, Any] = invocation.getArguments match {
          case args: Array[Object] =>
            new ChannelTransport[Any, Any](args.head.asInstanceOf[Channel])
        }
      }
    }
    when(codec.newClientDispatcher(any[Transport[Any, Any]])) thenAnswer {
      new Answer[SerialClientDispatcher[String, String]] {
        def answer(invocation: InvocationOnMock): SerialClientDispatcher[String, String] = {
          val arg = invocation.getArguments.head
          new SerialClientDispatcher[String, String](arg.asInstanceOf[Transport[String, String]])
        }
      }
    }

    when(codec.newTraceInitializer) thenReturn TraceInitializerFilter.clientModule[String, String]

    val clientAddress = new SocketAddress {}

    // Pipeline
    val clientPipelineFactory = mock[ChannelPipelineFactory]
    val channelPipeline = mock[ChannelPipeline]
    when(clientPipelineFactory.getPipeline) thenReturn channelPipeline
    when(codec.pipelineFactory) thenReturn clientPipelineFactory

    /*
        val codec = new Codec[String, String] {
          def pipelineFactory = clientPipelineFactory
        }
    */
    // Channel
    val channelFactory = mock[ChannelFactory]
    val channel = mock[Channel]
    val connectFuture = Mockito.spy(Channels.future(channel, true))
    val closeFuture = Mockito.spy(Channels.future(channel))
    when(channel.getCloseFuture) thenReturn closeFuture
    val channelConfig = new DefaultChannelConfig
    when(channel.getConfig()) thenReturn channelConfig
    when(channel.connect(clientAddress)) thenReturn connectFuture
    when(channel.getPipeline) thenReturn channelPipeline
    when(channelFactory.newChannel(channelPipeline)) thenReturn channel

    val codecFactory = Function.const(codec) _

    val clientBuilder = ClientBuilder()
      .name(name)
      .codec(codecFactory)
      .channelFactory(channelFactory)
      .hosts(Seq(clientAddress))
      .reportTo(statsReceiver)
      .hostConnectionLimit(1)

    def build() = clientBuilder.build()
    def buildFactory() = clientBuilder.buildFactory()
  }
}
