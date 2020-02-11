package com.twitter.finagle.http2

import com.twitter.finagle.Stack
import com.twitter.finagle.http2.transport.server.H2ServerFilter
import com.twitter.finagle.netty4.http.handler.UriValidatorHandler
import com.twitter.finagle.netty4.transport.ChannelTransport
import com.twitter.finagle.param.Timer
import io.netty.channel.ChannelPipeline
import io.netty.handler.codec.http2.Http2MultiplexHandler
import scala.jdk.CollectionConverters._

private[http2] object Http2PipelineInitializer {

  /**
   * Install Finagle specific filters and handlers common across all HTTP/2 only pipelines
   *
   * @param pipeline which to operate on.
   * @param params used to configure the server.
   */
  def setupServerPipeline(pipeline: ChannelPipeline, params: Stack.Params): Unit = {
    // we insert immediately after the Http2MultiplexHandler#0, which we know are the
    // last Http2 frames before they're converted to Http/1.1
    val timer = params[Timer].timer

    val codecName = pipeline
      .context(classOf[Http2MultiplexHandler])
      .name

    pipeline
      .addAfter(codecName, H2ServerFilter.HandlerName, new H2ServerFilter(timer, pipeline.channel))
      .remove(UriValidatorHandler)

    pruneDeadHandlers(pipeline)
  }

  private[this] def pruneDeadHandlers(pipeline: ChannelPipeline): Unit = {
    val deadPipelineEntries =
      pipeline.iterator.asScala
        .map(_.getKey)
        .dropWhile(_ != H2ServerFilter.HandlerName)
        .drop(1) // Now we're past the H2ServerFilter
        .takeWhile(_ != ChannelTransport.HandlerName)
        .toList

    deadPipelineEntries.foreach(pipeline.remove(_))
  }
}
