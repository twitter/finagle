package com.twitter.finagle.kestrel

import java.net.SocketAddress
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.builder.{Server => BuiltServer, ServerBuilder}
import protocol.Kestrel
import java.util.concurrent.LinkedBlockingDeque

class Server(address: SocketAddress) {

  private[this] val service = {
    val interpreter = new Interpreter(() => new LinkedBlockingDeque[ChannelBuffer])
    new InterpreterService(interpreter)
  }

  private[this] val serverSpec =
    ServerBuilder()
      .name("schmestrel")
      .codec(new Kestrel)
      .bindTo(address)

  private[this] var server: Option[BuiltServer] = None

  def start() {
    server = Some(serverSpec.build(service))
  }

  def stop() {
    require(server.isDefined, "Server is not open!")

    server.foreach { server =>
      server.close()
      this.server = None
    }
  }
}
