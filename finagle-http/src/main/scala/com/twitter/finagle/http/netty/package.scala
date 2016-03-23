package com.twitter.finagle.http

import com.twitter.finagle.{Http => HttpClient, Stack, ClientCodecConfig}
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.Http.param.ParameterizableTransporter
import com.twitter.finagle.netty3.Netty3Transporter
import com.twitter.finagle.param.Stats
import org.jboss.netty.channel.Channel

package object netty {
  private[this] val Netty3TransporterFn: Stack.Params => Transporter[Any, Any] = { params =>
    val com.twitter.finagle.param.Label(label) = params[com.twitter.finagle.param.Label]
    val codec = HttpClient.param.applyToCodec(params, Http())
      .client(ClientCodecConfig(label))
    val Stats(stats) = params[Stats]
    val newTransport = (ch: Channel) => codec.newClientTransport(ch, stats)
    Netty3Transporter(
      codec.pipelineFactory,
      params + Netty3Transporter.TransportFactory(newTransport))
  }

  val Netty3: ParameterizableTransporter = ParameterizableTransporter(Netty3TransporterFn)
}
