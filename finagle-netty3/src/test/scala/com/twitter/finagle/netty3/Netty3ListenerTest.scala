package com.twitter.finagle.netty3

import com.twitter.conversions.time._
import com.twitter.finagle.netty3.channel.{
  ChannelRequestStatsHandler,
  ChannelStatsHandler,
  WriteCompletionTimeoutHandler
}
import com.twitter.finagle.netty3.ssl.server.SslServerConnectHandler
import com.twitter.finagle.param.Label
import com.twitter.finagle.ssl.server.SslServerConfiguration
import com.twitter.finagle.stats.{InMemoryStatsReceiver, NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.Stack
import com.twitter.util.Duration
import javax.net.ssl.SSLEngine
import org.jboss.netty.channel.{Channels, ChannelHandler, ChannelPipeline, SimpleChannelHandler}
import org.jboss.netty.handler.ssl.SslHandler
import org.jboss.netty.handler.timeout.ReadTimeoutHandler
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import scala.collection.JavaConverters._
import scala.reflect.ClassTag

@RunWith(classOf[JUnitRunner])
class Netty3ListenerTest extends FunSuite with MockitoSugar {

  private[this] def findHandlerInPipeline[T <: ChannelHandler: ClassTag](
    pipeline: ChannelPipeline
  ): Option[T] = {
    val clazz = implicitly[ClassTag[T]].runtimeClass

    pipeline.toMap.asScala.values
      .find {
        case h if clazz.isInstance(h) => true
        case _ => false
      }
      .map(_.asInstanceOf[T])
  }

  // Netty3Listener.apply return a Listener[In, Out] but tests require a concrete Netty3Listener.
  private[this] def makeNetty3Listener(params: Stack.Params): Netty3Listener[Int, Int] = {
    val listener = Netty3Listener[Int, Int](Channels.pipelineFactory(Channels.pipeline()), params)
    listener.asInstanceOf[Netty3Listener[Int, Int]]
  }

  private[this] def makeListenerPipeline(
    params: Stack.Params,
    statsReceiver: StatsReceiver = NullStatsReceiver
  ): ChannelPipeline = {
    val listener = makeNetty3Listener(params)
    val pipelineFactory =
      listener.newServerPipelineFactory(statsReceiver, () => new SimpleChannelHandler)
    val pipeline = pipelineFactory.getPipeline()
    assert(pipeline != null)
    pipeline
  }

  test("creates a Netty3Listener instance based on Stack params") {
    val params = Stack.Params.empty
    val pipelineFactory = Channels.pipelineFactory(Channels.pipeline())
    val listener = Netty3Listener[Int, Int](pipelineFactory, params)
    assert(listener != null)
  }

  test("ChannelSnooper is not added by default") {
    val params = Stack.Params.empty + Label("name")
    val pipeline = makeListenerPipeline(params)
    val channelSnooper = findHandlerInPipeline[ChannelSnooper](pipeline)
    assert(channelSnooper.isEmpty)
  }

  test("ChannelSnooper is added when Verbose param is true") {
    val params = Stack.Params.empty + Label("name") + Transport.Verbose(true)
    val pipeline = makeListenerPipeline(params)
    val channelSnooper = findHandlerInPipeline[ChannelSnooper](pipeline)
    assert(channelSnooper.nonEmpty)
  }

  test("ChannelStatsHandler is not added when the statsReceiver is a NullStatsReceiver") {
    // The statsReceiver used is the one passed in to newServerPipelineFactory.
    // It is not determined by the Stats params.
    val params = Stack.Params.empty + Label("name")
    val pipeline = makeListenerPipeline(params, NullStatsReceiver)
    val statsHandler = findHandlerInPipeline[ChannelStatsHandler](pipeline)
    assert(statsHandler.isEmpty)
  }

  test("ChannelStatsHandler is added when the statsReceiver is not a NullStatsReceiver") {
    val params = Stack.Params.empty + Label("name")
    val pipeline = makeListenerPipeline(params, new InMemoryStatsReceiver)
    val statsHandler = findHandlerInPipeline[ChannelStatsHandler](pipeline)
    assert(statsHandler.nonEmpty)
  }

  test("ChannelRequestStatsHandler is not added when the statsReceiver is a NullStatsReceiver") {
    // Like ChannelStatsHandler, the statsReceiver used is the one passed
    // in to newServerPipelineFactory. It is not determined by the Stats params.
    val params = Stack.Params.empty + Label("name")
    val pipeline = makeListenerPipeline(params, NullStatsReceiver)
    val statsHandler = findHandlerInPipeline[ChannelRequestStatsHandler](pipeline)
    assert(statsHandler.isEmpty)
  }

  test("ChannelRequestStatsHandler is added when the statsReceiver is not a NullStatsReceiver") {
    val params = Stack.Params.empty + Label("name")
    val pipeline = makeListenerPipeline(params, new InMemoryStatsReceiver)
    val statsHandler = findHandlerInPipeline[ChannelRequestStatsHandler](pipeline)
    assert(statsHandler.nonEmpty)
  }

  test("ReadTimeoutHandler is not added by default") {
    val params = Stack.Params.empty + Label("name")
    val pipeline = makeListenerPipeline(params)
    val readHandler = findHandlerInPipeline[ReadTimeoutHandler](pipeline)
    assert(readHandler.isEmpty)
  }

  test("ReadTimeoutHandler is not added if the read timeout is Duration.Top") {
    val params = Stack.Params.empty + Label("name") +
      Transport.Liveness(Duration.Top, 1.second, None)
    val pipeline = makeListenerPipeline(params)
    val readHandler = findHandlerInPipeline[ReadTimeoutHandler](pipeline)
    assert(readHandler.isEmpty)
  }

  test("ReadTimeoutHandler is added if the read timeout is less than Duration.Top") {
    val params = Stack.Params.empty + Label("name") +
      Transport.Liveness(1.second, Duration.Top, None)
    val pipeline = makeListenerPipeline(params)
    val readHandler = findHandlerInPipeline[ReadTimeoutHandler](pipeline)
    assert(readHandler.nonEmpty)
  }

  test("WriteCompletionTimeoutHandler is not added by default") {
    val params = Stack.Params.empty + Label("name")
    val pipeline = makeListenerPipeline(params)
    val writeHandler = findHandlerInPipeline[WriteCompletionTimeoutHandler](pipeline)
    assert(writeHandler.isEmpty)
  }

  test("WriteCompletionTimeoutHandler is not added if the write timeout is Duration.Top") {
    val params = Stack.Params.empty + Label("name") +
      Transport.Liveness(1.second, Duration.Top, None)
    val pipeline = makeListenerPipeline(params)
    val writeHandler = findHandlerInPipeline[WriteCompletionTimeoutHandler](pipeline)
    assert(writeHandler.isEmpty)
  }

  test("WriteCompletionTimeoutHandler is added if the write timeout is less than Duration.Top") {
    val params = Stack.Params.empty + Label("name") +
      Transport.Liveness(Duration.Top, 1.second, None)
    val pipeline = makeListenerPipeline(params)
    val writeHandler = findHandlerInPipeline[WriteCompletionTimeoutHandler](pipeline)
    assert(writeHandler.nonEmpty)
  }

  test("SslHandler is not added by default") {
    val params = Stack.Params.empty + Label("name")
    val pipeline = makeListenerPipeline(params)
    val sslHandler = findHandlerInPipeline[SslHandler](pipeline)
    assert(sslHandler.isEmpty)
  }

  test("SslHandler is not added if the SSL/TLS server configuration param is None") {
    val params = Stack.Params.empty + Label("name") + Transport.ServerSsl(None)
    val pipeline = makeListenerPipeline(params)
    val sslHandler = findHandlerInPipeline[SslHandler](pipeline)
    assert(sslHandler.isEmpty)
  }

  test("SslHandler is added if the SSL/TLS server configuration param is configured") {
    val engine = mock[SSLEngine]
    val params = Stack.Params.empty + Label("name") +
      Transport.ServerSsl(Some(SslServerConfiguration()))
    val pipeline = makeListenerPipeline(params)
    val sslHandler = findHandlerInPipeline[SslHandler](pipeline)
    assert(sslHandler.nonEmpty)
  }

  test("SslServerConnectHandler is not added by default") {
    val params = Stack.Params.empty + Label("name")
    val pipeline = makeListenerPipeline(params)
    val sslConnectHandler = findHandlerInPipeline[SslServerConnectHandler](pipeline)
    assert(sslConnectHandler.isEmpty)
  }

  test("SslServerConnectHandler is not added if the SSL/TLS server config param is None") {
    val params = Stack.Params.empty + Label("name") + Transport.ServerSsl(None)
    val pipeline = makeListenerPipeline(params)
    val sslConnectHandler = findHandlerInPipeline[SslServerConnectHandler](pipeline)
    assert(sslConnectHandler.isEmpty)
  }

  test("SslServerConnectHandler is added if the SSL/TLS server config param is configured") {
    val engine = mock[SSLEngine]
    val params = Stack.Params.empty + Label("name") +
      Transport.ServerSsl(Some(SslServerConfiguration()))
    val pipeline = makeListenerPipeline(params)
    val sslConnectHandler = findHandlerInPipeline[SslServerConnectHandler](pipeline)
    assert(sslConnectHandler.nonEmpty)
  }

  test("FinagleBridge is added by default") {
    class TestBridgeHandler extends SimpleChannelHandler {}

    val testBridgeHandler = new TestBridgeHandler()
    val params = Stack.Params.empty + Label("name")

    val listener = makeNetty3Listener(params)
    val pipelineFactory =
      listener.newServerPipelineFactory(NullStatsReceiver, () => testBridgeHandler)
    val pipeline = pipelineFactory.getPipeline()

    val bridgeHandler = findHandlerInPipeline[TestBridgeHandler](pipeline)
    assert(bridgeHandler.nonEmpty)
    assert(bridgeHandler.exists(_ == testBridgeHandler))
  }

}
