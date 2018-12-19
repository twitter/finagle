package com.twitter.finagle.example.http

import com.twitter.concurrent.AsyncStream
import com.twitter.conversions.DurationOps._
import com.twitter.finagle.http.{Request, Response, Status}
import com.twitter.finagle.{Http, Service}
import com.twitter.io.{Buf, Pipe}
import com.twitter.util.{Await, Future, JavaTimer}
import scala.util.Random

/**
 * An example of a streaming HTTP server using chunked transfer encoding.
 */
object HttpStreamingServer {
  val random = new Random
  implicit val timer = new JavaTimer

  // Int, sleep, repeat.
  def ints(): AsyncStream[Int] =
    random.nextInt +::
      AsyncStream.fromFuture(Future.sleep(100.millis)).flatMap(_ => ints())

  def main(args: Array[String]): Unit = {
    val service = new Service[Request, Response] {
      // Only one stream exists.
      @volatile private[this] var messages: AsyncStream[Buf] =
        ints().map(n => Buf.Utf8(n.toString))

      // Allow the head of the stream to be collected.
      messages.foreach(_ => messages = messages.drop(1))

      def apply(request: Request): Future[Response] = {
        val writable = new Pipe[Buf]()
        // Start writing thread.
        messages.foreachF(writable.write)
        Future.value(Response(request.version, Status.Ok, writable))
      }
    }

    Await.result(
      Http.server
      // Translate buffered writes into HTTP chunks.
        .withStreaming(enabled = true)
        // Listen on port 8080.
        .serve("0.0.0.0:8080", service)
    )
  }
}
