package com.twitter.finagle.example.stream

import com.twitter.concurrent.{Broker, Offer}
import com.twitter.conversions.time._
import com.twitter.finagle.Service
import com.twitter.finagle.builder.{Server, ServerBuilder}
import com.twitter.finagle.stream.{Stream, StreamRequest, StreamResponse}
import com.twitter.io.Buf
import com.twitter.util.{Future, Timer, JavaTimer}
import java.net.InetSocketAddress
import scala.util.Random

/**
 * An example of a streaming server using HTTP Chunking. The Stream
 * Codec uses HTTP Chunks and newline delimited items.
 */
object StreamServer {
  // "tee" messages across all of the registered brokers.
  val addBroker = new Broker[Broker[Buf]]
  val remBroker = new Broker[Broker[Buf]]
  val messages = new Broker[Buf]
  private[this] def tee(receivers: Set[Broker[Buf]]) {
    Offer.select(
      addBroker.recv { b => tee(receivers + b) },
      remBroker.recv { b => tee(receivers - b) },
      if (receivers.isEmpty) Offer.never else {
        messages.recv { m =>
          Future.join(receivers.map { _ ! m }.toSeq) ensure tee(receivers)
        }
      }
    )
  }

  private[this] def produce(r: Random, t: Timer) {
    t.schedule(1.second.fromNow) {
      val m = Buf.Utf8(r.nextInt.toString + "\n")
      messages.send(m) andThen produce(r, t)
    }
  }

  // start the two processes.
  tee(Set())
  produce(new Random, new JavaTimer)

  def main(args: Array[String]) {
    val myService = new Service[StreamRequest, StreamResponse] {
      def apply(request: StreamRequest) = Future {
        val subscriber = new Broker[Buf]
        addBroker ! subscriber
        new StreamResponse {
          val info = StreamResponse.Info(request.version, StreamResponse.Status(200), Nil)
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
