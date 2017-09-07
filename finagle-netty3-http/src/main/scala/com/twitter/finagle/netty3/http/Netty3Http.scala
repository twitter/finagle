package com.twitter.finagle.netty3.http

import com.twitter.finagle.http.param._
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.netty3.{Netty3Listener, Netty3Transporter}
import com.twitter.finagle.param.{Label, Stats}
import com.twitter.finagle.server.Listener
import com.twitter.finagle.transport.TransportContext
import com.twitter.finagle.{ServerCodecConfig, Stack, ClientCodecConfig}
import java.net.SocketAddress
import org.jboss.netty.channel.Channel

object Netty3Http {
  private[this] def applyToCodec(params: Stack.Params, codec: HttpCodecFactory): HttpCodecFactory =
    codec
      .maxRequestSize(params[MaxRequestSize].size)
      .maxResponseSize(params[MaxResponseSize].size)
      .streaming(params[Streaming].enabled)
      .decompressionEnabled(params[Decompression].enabled)
      .compressionLevel(params[CompressionLevel].level)
      .maxInitialLineLength(params[MaxInitialLineSize].size)
      .maxHeaderSize(params[MaxHeaderSize].size)

  private[finagle] val Transporter
    : Stack.Params => SocketAddress => Transporter[Any, Any, TransportContext] = { params =>
    val Label(label) = params[Label]
    val codec = applyToCodec(params, HttpCodecFactory())
      .client(ClientCodecConfig(label))
    val Stats(stats) = params[Stats]
    val newTransport = (ch: Channel) => codec.newClientTransport(ch, stats)
    Netty3Transporter(
      codec.pipelineFactory,
      _,
      params + Netty3Transporter.TransportFactory(newTransport)
    )
  }

  private[finagle] val Listener: Stack.Params => Listener[Any, Any, TransportContext] = { params =>
    val Label(label) = params[Label]
    val httpPipeline =
      applyToCodec(params, HttpCodecFactory())
        .server(ServerCodecConfig(label, new SocketAddress {}))
        .pipelineFactory

    Netty3Listener(httpPipeline, params)
  }
}
