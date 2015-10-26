package com.twitter.finagle.example.http

import com.twitter.concurrent.AsyncStream
import com.twitter.util.{Await, Base64StringEncoder => Base64, Future}
import com.twitter.finagle.http.{Request, Method, Status}
import com.twitter.finagle.Http
import com.twitter.io.{Buf, Reader}

/**
 * This client connects to a Streaming HTTP service, prints 1000 messages, then
 * disconnects.  If you start two or more of these clients simultaneously, you
 * will notice that this is also a PubSub example.
 */
object HttpStreamingClient {
  def main(args: Array[String]): Unit = {
    val username = args(0)
    val password = args(1)
    val host = args(2)
    val path = args(3)

    val client = Http.client.withStreaming(enabled = true).newService(host)

    val request = Request(Method.Get, path)
    val userpass = username + ":" + password
    request.headerMap.add("Authorization", "Basic " + Base64.encode(userpass.getBytes("UTF-8")))
    request.headerMap.add("User-Agent", "Finagle 0.0")
    request.headerMap.add("Host", host)
    println(request)

    Await.result(client(request).flatMap {
      case response if response.status != Status.Ok =>
        println(response)
        client.close()

      case response =>
        var messageCount = 0 // Wait for 1000 messages then shut down.
        fromReader(response.reader).foreach {
          case Buf.Utf8(buf) if messageCount < 1000 =>
            messageCount += 1
            println(buf)
            println("--")

          case _ =>
            client.close()
        }
    })
  }

  def fromReader(reader: Reader): AsyncStream[Buf] =
    AsyncStream.fromFuture(reader.read(Int.MaxValue)).flatMap {
      case None => AsyncStream.empty
      case Some(a) => a +:: fromReader(reader)
    }
}
