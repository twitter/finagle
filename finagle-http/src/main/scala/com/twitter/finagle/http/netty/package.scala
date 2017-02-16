package com.twitter.finagle.http

import com.twitter.finagle.http.param._
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.http.{Http => HttpCodec}
import com.twitter.finagle.netty3.{Netty3Listener, Netty3Transporter}
import com.twitter.finagle.param.{Label, Stats}
import com.twitter.finagle.server.Listener
import com.twitter.finagle.{ServerCodecConfig, Stack, ClientCodecConfig}
import java.net.SocketAddress
import org.jboss.netty.channel.Channel

package object netty {
  private[this] def applyToCodec(params: Stack.Params, codec: HttpCodec): HttpCodec =
    codec
      .maxRequestSize(params[MaxRequestSize].size)
      .maxResponseSize(params[MaxResponseSize].size)
      .streaming(params[Streaming].enabled)
      .decompressionEnabled(params[Decompression].enabled)
      .compressionLevel(params[CompressionLevel].level)
      .maxInitialLineLength(params[MaxInitialLineSize].size)
      .maxHeaderSize(params[MaxHeaderSize].size)


  private[finagle] val Netty3HttpTransporter: Stack.Params => SocketAddress => Transporter[Any, Any] = { params =>
    val Label(label) = params[Label]
    val codec = applyToCodec(params, Http())
      .client(ClientCodecConfig(label))
    val Stats(stats) = params[Stats]
    val newTransport = (ch: Channel) => codec.newClientTransport(ch, stats)
    Netty3Transporter(
      codec.pipelineFactory,
      _,
      params + Netty3Transporter.TransportFactory(newTransport))
  }

  private[finagle] val Netty3HttpListener: Stack.Params => Listener[Any, Any] = { params =>
    val Label(label) = params[Label]
    val httpPipeline =
      applyToCodec(params, HttpCodec())
        .server(ServerCodecConfig(label, new SocketAddress{}))
        .pipelineFactory

    Netty3Listener(httpPipeline, params)
  }
}
