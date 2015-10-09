package com.twitter.finagle.memcached.integration

import _root_.java.net.SocketAddress
import com.twitter.finagle.builder.{Server => BuiltServer, ServerBuilder}
import com.twitter.finagle.memcached.protocol.text.Memcached
import com.twitter.finagle.memcached.util.AtomicMap
import com.twitter.finagle.memcached.{Entry, Interpreter, InterpreterService}
import com.twitter.io.Buf
import com.twitter.util.{Await, SynchronizedLruMap}

class InProcessMemcached(address: SocketAddress) {
  val concurrencyLevel = 16
  val slots = 500000
  val slotsPerLru = slots / concurrencyLevel
  val maps = (0 until concurrencyLevel) map { i =>
    new SynchronizedLruMap[Buf, Entry](slotsPerLru)
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
