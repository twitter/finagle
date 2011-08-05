package com.twitter.finagle.kestrel

import scala.collection.JavaConversions._
import _root_.java.{util => ju}

import com.twitter.concurrent.{Offer, Broker}

object AllHandlesDiedException extends Exception

/**
 * Read from multiple clients in round-robin fashion, "grabby hands"
 * style.  The load balancing is simple, and falls out naturally from
 * the user of the {{Offer}} mechanism: When there are multiple
 * available messages, round-robin across them.  Otherwise, wait for
 * the first message to arrive.
 */
object MultiReader {
  def apply(clients: Seq[Client], queueName: String): ReadHandle =
    apply(clients map { _.readReliably(queueName) })

  /**
   * A java friendly interface: we use scala's implicit conversions to
   * feed in a {{java.util.Iterator<ReadHandle>}}
   */
  def apply(handles: ju.Iterator[ReadHandle]): ReadHandle =
    apply(handles.toSeq)

  def apply(handles: Seq[ReadHandle]): ReadHandle = {
    val error = new Broker[Throwable]
    val messages = new Broker[ReadMessage]
    val close = new Broker[Unit]

    var underlying = Set() ++ handles
    val errors = underlying map { h => h.error map { _ => h } } toSeq

    val closeOf = close.recv { _ =>
      underlying foreach { _.close() }
      error ! ReadClosedException
    }

    def loop() {
      // compute the active queues
      if (underlying.isEmpty) {
        error ! AllHandlesDiedException
        return
      }
      val queues = underlying map { _.messages } toSeq

      // we sequence here to ensure that close gets
      // priority over reads
      val of = closeOf orElse {
        Offer.choose(
          closeOf,
          Offer.choose(queues:_*) { m =>
            messages ! m
            loop()
          },
          Offer.choose(errors:_*) { h =>
            underlying -= h
            loop()
          }
        )
      }

      of()
    }

    loop()

    ReadHandle(messages.recv, error.recv, close.send(()))
  }
}
