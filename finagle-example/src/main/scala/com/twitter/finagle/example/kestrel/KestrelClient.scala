package com.twitter.finagle.example.kestrel

import com.twitter.conversions.time._
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.kestrel.protocol.Kestrel
import com.twitter.finagle.kestrel.{ReadHandle, Client}
import com.twitter.io.{Buf, Charsets}
import com.twitter.util.JavaTimer
import com.twitter.finagle.service.Backoff
import java.util.concurrent.atomic.AtomicBoolean

/**
 * This example show how to configure a client doing reliable reads
 * from multiple kestrel servers.
 *
 * This shows how to process messages continuously from a kestrel queue.
 */
object KestrelClient {

  def main(args: Array[String]) {
    // Configure your duration as necessary.
    // On a production server, you would typically not use this part and just
    // let the client read continuously until a shutdown is requested.
    val duration = 10.seconds
    println("running for %s".format(duration))

    // Add "host:port" pairs as needed
    val hosts = Seq("localhost:22133")
    val stopped = new AtomicBoolean(false)

    val clients: Seq[Client] = hosts map { host =>
      Client(ClientBuilder()
        .codec(Kestrel())
        .hosts(host)
        .hostConnectionLimit(1) // process at most 1 item per connection concurrently
        .buildFactory())
    }

    val readHandles: Seq[ReadHandle] = {
      val queueName = "queue"
      val timer = new JavaTimer(isDaemon = true)
      val retryBackoffs = Backoff.const(10.milliseconds)
      clients map { _.readReliably(queueName, timer, retryBackoffs) }
    }

    val readHandle: ReadHandle = ReadHandle.merged(readHandles)

    // Attach an async error handler that prints to stderr
    readHandle.error foreach { e =>
      if (!stopped.get) System.err.println("zomg! got an error " + e)
    }

    // Attach an async message handler that prints the messages to stdout
    readHandle.messages foreach { msg =>
      try {
        val Buf.Utf8(str) = msg.bytes 
        println(str)
      } finally {
        msg.ack.sync() // if we don't do this, no more msgs will come to us
      }
    }

    // Let it run for a little while
    Thread.sleep(duration.inMillis)
    // Without this, we get messages sent to our error handler
    stopped.set(true)

    println("stopping")
    readHandle.close()
    clients foreach { _.close() }
    println("done")
  }

}
