package com.twitter.finagle.memcached.integration.external

import _root_.java.net.SocketAddress
import com.twitter.finagle.Memcached.Server
import com.twitter.finagle.{ListeningServer, Memcached}
import com.twitter.finagle.memcached.util.AtomicMap
import com.twitter.finagle.memcached.{Entry, Interpreter, InterpreterService}
import com.twitter.io.Buf
import com.twitter.util.{Await, SynchronizedLruMap}

class InProcessMemcached(address: SocketAddress) {
  val concurrencyLevel: Int = 16
  val slots: Int = 500000
  val slotsPerLru: Int = slots / concurrencyLevel
  val maps: Seq[SynchronizedLruMap[Buf, Entry]] = (0 until concurrencyLevel) map { i =>
    new SynchronizedLruMap[Buf, Entry](slotsPerLru)
  }

  private[this] val service: InterpreterService = {
    val interpreter = new Interpreter(new AtomicMap(maps))
    new InterpreterService(interpreter)
  }

  private[this] val serverSpec: Server = Memcached.server.withLabel("finagle")

  private[this] var server: Option[ListeningServer] = None

  def start(): ListeningServer = {
    server = Some(serverSpec.serve(address, service))
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
