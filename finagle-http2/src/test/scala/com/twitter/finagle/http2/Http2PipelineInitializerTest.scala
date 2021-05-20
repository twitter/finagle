package com.twitter.finagle.http2

import com.twitter.finagle.Stack
import com.twitter.finagle.http2.transport.server.H2ServerFilter
import com.twitter.finagle.netty4.http.handler.UriValidatorHandler
import com.twitter.finagle.netty4.transport.ChannelTransport
import io.netty.channel.ChannelHandlerAdapter
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.codec.http2.Http2MultiplexHandler
import org.scalatest.funsuite.AnyFunSuite

class Http2PipelineInitializerTest extends AnyFunSuite {

  private[this] class TestHandler extends ChannelHandlerAdapter

  private[this] def testChannel: EmbeddedChannel = {
    val em = new EmbeddedChannel()
    val (frameCodec, multiplexHandler) =
      MultiplexHandlerBuilder.serverFrameCodec(Stack.Params.empty, new ChannelHandlerAdapter {})
    // We put the uri validation handler up front so it doesn't get caught in the prune logic
    // and thus we can test this.
    em.pipeline.addLast(UriValidatorHandler.HandlerName, UriValidatorHandler)
    em.pipeline.addLast(frameCodec, multiplexHandler)
    em
  }

  test("adds a H2ServerFilter to the pipeline") {
    val em = testChannel
    Http2PipelineInitializer.setupServerPipeline(em.pipeline, Stack.Params.empty)
    assert(em.pipeline.get(classOf[H2ServerFilter]) != null)
    assert(em.pipeline.get(classOf[Http2MultiplexHandler]) != null)
  }

  test("remove the UriValidationHandler") {
    val em = testChannel
    assert(em.pipeline.get(UriValidatorHandler.HandlerName) != null)
    Http2PipelineInitializer.setupServerPipeline(em.pipeline, Stack.Params.empty)
    assert(em.pipeline.get(UriValidatorHandler.HandlerName) == null)
  }

  test("prunes handlers that came after the existing Http2MultiplexHandler") {
    val em = testChannel
    em.pipeline.addLast(new TestHandler)
    Http2PipelineInitializer.setupServerPipeline(em.pipeline, Stack.Params.empty)

    assert(em.pipeline.get(classOf[H2ServerFilter]) != null)
    assert(em.pipeline.get(classOf[Http2MultiplexHandler]) != null)
    assert(em.pipeline.get(classOf[TestHandler]) == null)
  }

  test("doesn't prune the channel transport handler") {
    val em = testChannel
    em.pipeline.addLast(new TestHandler)
    em.pipeline.addLast(ChannelTransport.HandlerName, new ChannelHandlerAdapter {})
    Http2PipelineInitializer.setupServerPipeline(em.pipeline, Stack.Params.empty)

    assert(em.pipeline.get(classOf[H2ServerFilter]) != null)
    assert(em.pipeline.get(classOf[Http2MultiplexHandler]) != null)
    assert(em.pipeline.get(ChannelTransport.HandlerName) != null)
  }
}
