package com.twitter.finagle.thrift.transport.netty3

import com.twitter.finagle.{Stack, Thrift}
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.netty3.{Netty3Listener, Netty3Transporter}
import com.twitter.finagle.param.Label
import com.twitter.finagle.server.Listener
import com.twitter.finagle.thrift.ThriftClientRequest
import java.net.SocketAddress

/**
 * Netty3 [[Transporter]] and [[Listener]] builder implementations
 *
 * The method of message segmentation and the thrift protocol are obtained from the provided
 * [[Stack.Params]]. See the [[Thrift]] object for more details.
 */
private[finagle] object Netty3Transport {
  val Client: Stack.Params => SocketAddress => Transporter[ThriftClientRequest, Array[Byte]] = { params =>
    val Thrift.param.Framed(framed) = params[Thrift.param.Framed]

    val pipeline =
      if (framed) ThriftClientFramedPipelineFactory
      else {
        val Thrift.param.ProtocolFactory(protocolFactory) = params[Thrift.param.ProtocolFactory]
        ThriftClientBufferedPipelineFactory(protocolFactory)
      }
    Netty3Transporter(pipeline, _, params)
  }

  val Server: Stack.Params => Listener[Array[Byte], Array[Byte]] = { params =>
    val Thrift.param.Framed(framed) = params[Thrift.param.Framed]

    val pipeline =
      if (framed) ThriftServerFramedPipelineFactory
      else {
        val Thrift.param.ProtocolFactory(protocolFactory) = params[Thrift.param.ProtocolFactory]
        ThriftServerBufferedPipelineFactory(protocolFactory)
      }

    Netty3Listener(pipeline,
      if (params.contains[Label]) params else params + Label("thrift"))
  }
}
