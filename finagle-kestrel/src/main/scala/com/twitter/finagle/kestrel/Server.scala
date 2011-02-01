package com.twitter.finagle.kestrel

import _root_.java.net.SocketAddress
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.builder.{Server => BuiltServer, ServerBuilder}
import protocol.Kestrel
import _root_.java.util.concurrent.{BlockingDeque, LinkedBlockingDeque}
import com.twitter.util.MapMaker

class Server(address: SocketAddress) {
  private[this] val serviceFactory = {
    val queues = MapMaker[ChannelBuffer, BlockingDeque[ChannelBuffer]] { config =>
      config.compute { key =>
        new LinkedBlockingDeque[ChannelBuffer]
      }
    }

    () => {
      new InterpreterService(new Interpreter(queues))
    }
  }

  private[this] val serverSpec =
    ServerBuilder()
      .name("schmestrel")
      .codec(new Kestrel)
      .bindTo(address)

  private[this] var server: Option[BuiltServer] = None

  def start() {
    server = Some(serverSpec.build(serviceFactory))
  }

  def stop() {
    require(server.isDefined, "Server is not open!")

    server.foreach { server =>
      server.close()
      this.server = None
    }
  }
}
