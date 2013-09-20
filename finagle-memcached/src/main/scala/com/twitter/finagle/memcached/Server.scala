package com.twitter.finagle.memcached

import _root_.java.net.SocketAddress
import com.twitter.finagle.builder.{Server => BuiltServer, ServerBuilder}
import com.twitter.finagle.memcached.protocol.text.Memcached
import com.twitter.finagle.memcached.util.AtomicMap
import com.twitter.util.{Await, SynchronizedLruMap}
import org.jboss.netty.buffer.ChannelBuffer

/**
 * An in-process memcached server.
 */
@deprecated("Moved into test", "7.0.0")
class Server(address: SocketAddress) {
  val concurrencyLevel = 16
  val slots = 500000
  val slotsPerLru = slots / concurrencyLevel
  val maps = (0 until concurrencyLevel) map { i =>
    new SynchronizedLruMap[ChannelBuffer, Entry](slotsPerLru)
  }

  private[this] val service = {
    val interpreter = new Interpreter(new AtomicMap(maps))
    new InterpreterService(interpreter)
  }

  private[this] val serverSpec =
    ServerBuilder()
      .name("finagle")
      .codec(Memcached())
      .bindTo(address)

  private[this] var server: Option[BuiltServer] = None

  def start(): BuiltServer = {
    server = Some(serverSpec.build(service))
    server.get
  }

  def stop(blocking: Boolean = false) {
    server.foreach { server =>
      if (blocking) Await.result(server.close())
      else server.close()
      this.server = None
    }
  }
}
