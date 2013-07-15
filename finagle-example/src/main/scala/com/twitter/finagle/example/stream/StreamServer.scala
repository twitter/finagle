package com.twitter.finagle.example.stream

import com.twitter.concurrent.{Broker, Offer}
import com.twitter.finagle.builder.{Server, ServerBuilder}
import com.twitter.finagle.Service
import com.twitter.finagle.stream.{Stream, StreamResponse}
import com.twitter.util.{Future, Timer, JavaTimer}
import com.twitter.conversions.time._
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
  // "tee" messages across all of the registered brokers.
  val addBroker = new Broker[Broker[ChannelBuffer]]
  val remBroker = new Broker[Broker[ChannelBuffer]]
  val messages = new Broker[ChannelBuffer]
  private[this] def tee(receivers: Set[Broker[ChannelBuffer]]) {
    Offer.select(
      addBroker.recv { b => tee(receivers + b) },
      remBroker.recv { b => tee(receivers - b) },
      if (receivers.isEmpty) Offer.never else {
        messages.recv { m =>
          Future.join(receivers map { _ ! m } toSeq) ensure tee(receivers)
        }
      }
    )
  }

  private[this] def produce(r: Random, t: Timer) {
    t.schedule(1.second.fromNow) {
      val m = copiedBuffer(r.nextInt.toString + "\n", CharsetUtil.UTF_8)
      messages.send(m) andThen produce(r, t)
    }
  }

  // start the two processes.
  tee(Set())
  produce(new Random, new JavaTimer)

  def main(args: Array[String]) {
    val myService = new Service[HttpRequest, StreamResponse] {
      def apply(request: HttpRequest) = Future {
        val subscriber = new Broker[ChannelBuffer]
        addBroker ! subscriber
        new StreamResponse {
          val httpResponse = new DefaultHttpResponse(
            request.getProtocolVersion, HttpResponseStatus.OK)
          def messages = subscriber.recv
          def error = new Broker[Throwable].recv
          def release() = {
            remBroker ! subscriber
            // sink any existing messages, so they
            // don't hold up the upstream.
            subscriber.recv foreach { _ => () }
          }
        }
      }
    }

    val server: Server = ServerBuilder()
      .codec(Stream())
      .bindTo(new InetSocketAddress(8080))
      .name("streamserver")
      .build(myService)
  }
}
