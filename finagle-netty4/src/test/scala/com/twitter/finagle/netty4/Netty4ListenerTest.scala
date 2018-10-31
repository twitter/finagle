package com.twitter.finagle.netty4

import com.twitter.finagle.Stack.Params
import com.twitter.finagle.dispatch.SerialServerDispatcher
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.{ListeningServer, Service}
import io.netty.channel.ChannelPipeline
import java.net.SocketAddress

class Netty4ListenerTest extends AbstractNetty4ListenerTest {

  protected def serveService[Req: Manifest, Rep: Manifest](
    address: SocketAddress,
    init: ChannelPipeline => Unit,
    params: Params,
    service: Service[Req, Rep]
  ): ListeningServer = {
    val listener = Netty4Listener[Rep, Req](init, params)
    listener.listen(address) { t: Transport[Rep, Req] =>
      new SerialServerDispatcher[Req, Rep](t, service)
    }
  }
}
