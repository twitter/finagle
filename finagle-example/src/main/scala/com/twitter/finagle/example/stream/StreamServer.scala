package com.twitter.finagle.example.stream

import com.twitter.concurrent.{ChannelSource, Channel}
import com.twitter.finagle.builder.{Server, ServerBuilder}
import com.twitter.finagle.Service
import com.twitter.finagle.stream.{Stream, StreamResponse}
import com.twitter.util.Future
import java.net.InetSocketAddress
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBuffers.copiedBuffer
import org.jboss.netty.handler.codec.http.{DefaultHttpResponse, HttpRequest, HttpResponseStatus}
import org.jboss.netty.util.CharsetUtil
import scala.util.Random

/**
 * An example of a streaming server using HTTP Chunking. The Stream
 * Codec uses HTTP Chunks and newline delimited items.
 */
object StreamServer {
  // A ChannelSource is a writable Channel
  val channelSource = new ChannelSource[ChannelBuffer]

  /**
   * Issue messages on the Channel so long as clients are connected
   */
  class ProducerThread extends Thread {
    val rand = new Random

    override def run() {
      while (!isInterrupted) {
        val message = copiedBuffer(rand.nextInt.toString, CharsetUtil.UTF_8)

        // Throttle message delivery by ensuring that all clients
        // have processed the message before sending the next one:
        Future.join(channelSource.send(message))()
      }
    }
  }

  // Note: the memory consistency effects of `respond` do not require this
  // variable to annotated @volatile.
  var producer: ProducerThread = null

  def main(args: Array[String]) {
    val myService = new Service[HttpRequest, StreamResponse] {
      def apply(request: HttpRequest) = Future {
        new StreamResponse {
          val httpResponse = new DefaultHttpResponse(request.getProtocolVersion, HttpResponseStatus.OK)
          def channel = channelSource
          def release() = ()
        }
      }
    }

    val server: Server = ServerBuilder()
      .codec(Stream())
      .bindTo(new InetSocketAddress(8080))
      .name("streamserver")
      .build(myService)

    // Start sending when there is at least one observer.
    // Stop sending when the number of observers goes to zero.
    channelSource.numObservers.respond { i =>
      i match {
        case 1 =>
          producer = new ProducerThread
          producer.start()
        case 0 =>
          producer.interrupt()
          producer = null
        case _ =>
      }
      Future.Done
    }
  }
}