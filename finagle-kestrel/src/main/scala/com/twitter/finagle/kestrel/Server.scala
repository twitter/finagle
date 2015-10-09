package com.twitter.finagle.kestrel

import _root_.java.net.SocketAddress
import _root_.java.util.concurrent.{BlockingDeque, LinkedBlockingDeque}
import com.google.common.cache.{CacheBuilder, CacheLoader}
import com.twitter.finagle.builder.{Server => BuiltServer, ServerBuilder}
import com.twitter.finagle.{ServiceFactory, ClientConnection}
import com.twitter.io.Buf
import com.twitter.util.{Future, Time}
import protocol.{Kestrel, Command, Response}

class Server(address: SocketAddress) {
  private[this] val serviceFactory = new ServiceFactory[Command, Response] {

    private[this] val queues = CacheBuilder.newBuilder()
      .build(new CacheLoader[Buf, BlockingDeque[Buf]] {
        def load(k: Buf) = new LinkedBlockingDeque[Buf]
      })

    def apply(conn: ClientConnection) = Future.value(new InterpreterService(new Interpreter(queues)))
    def close(deadline: Time) = Future.Done
  }

  private[this] val serverSpec =
    ServerBuilder()
      .name("schmestrel")
      .codec(Kestrel())
      .bindTo(address)

  private[this] var server: Option[BuiltServer] = None

  def start(): BuiltServer = {
    server = Some(serverSpec.build(serviceFactory))
    server.get
  }

  def stop() {
    require(server.isDefined, "Server is not open!")

    server.foreach { server =>
      server.close()
      this.server = None
    }
  }
}
