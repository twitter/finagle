package com.twitter.finagle.memcached
import _root_.java.net.SocketAddress
import org.jboss.netty.channel.Channel
import com.twitter.util.SynchronizedLruMap
import org.jboss.netty.buffer.ChannelBuffer
import util.AtomicMap
import com.twitter.finagle.builder.ServerBuilder
import protocol.text.Memcached

class Server(address: SocketAddress) {
  val concurrencyLevel = 16
  val slots = 500000
  val slotsPerLru = slots / concurrencyLevel
  val maps = (0 until concurrencyLevel) map { i =>
    new SynchronizedLruMap[ChannelBuffer, ChannelBuffer](slotsPerLru)
  }

  private[this] val service = {
    val interpreter = new Interpreter(new AtomicMap(maps))
    new InterpreterService(interpreter)
  }

  private[this] val serverSpec =
    ServerBuilder()
      .name("schmemcached")
      .codec(new Memcached)
      .service(service)
      .bindTo(address)

  private[this] var channel: Option[Channel] = None

  def start() {
    channel = Some(serverSpec.build())
  }

  def stop() {
    require(channel.isDefined, "Channel is not open!")

    channel.foreach { channel =>
      channel.close()
      this.channel = None
    }
  }
}