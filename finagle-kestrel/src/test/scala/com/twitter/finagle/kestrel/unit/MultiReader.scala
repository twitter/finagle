package com.twitter.finagle.kestrel.unit

import org.specs.Specification
import org.specs.mock.Mockito

import scala.collection.mutable.ArrayBuffer

import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import com.twitter.concurrent.{Broker, Offer}
import com.twitter.util.Time
import com.twitter.conversions.time._
import com.twitter.finagle.kestrel._

object MultiReaderSpec extends Specification with Mockito {
  noDetailedDiffs()

  class MockHandle extends ReadHandle {
    val _messages = new Broker[ReadMessage]
    val _error = new Broker[Throwable]

    val messages = _messages.recv
    val error = _error.recv
    def close() {} // to spy on!
  }

  "MultiReader" should {
    val N = 3
    val handles = 0 until N map { _ => spy(new MockHandle) }

    "always grab the first available message" in {
      val handle = MultiReader(handles)

      val messages = new ArrayBuffer[ReadMessage]
      handle.messages foreach { messages += _ }

      // stripe some messages across
      val sentMessages = 0 until N*100 map { _ => mock[ReadMessage] }

      messages must beEmpty
      sentMessages.zipWithIndex foreach { case (m, i) =>
        handles(i % handles.size)._messages ! m
      }

      messages must be_==(sentMessages)
   }

   // we use frozen time for deterministic randomness.
   // the order was simply determined empirically.
   "round robin from multiple available queues" in Time.withTimeAt(Time.epoch + 1.seconds) { _ =>
     // stuff the queues beforehand
     val ms = handles map { h =>
       val m = mock[ReadMessage]
       h._messages ! m
       m
     }

     val handle = MultiReader(handles)
     val messages = new ArrayBuffer[ReadMessage]
     (handle.messages??) must be_==(ms(2))
     (handle.messages??) must be_==(ms(0))
     (handle.messages??) must be_==(ms(1))
   }

   "propagate closes" in {
     handles foreach { h => there was no(h).close() }
     val handle = MultiReader(handles)
     handle.close()
     handles foreach { h => there was one(h).close() }
   }

   "propagate errors when everything's errored out" in {
     val handle = MultiReader(handles)
     val e = (handle.error?)
     handles foreach { h =>
       e.isDefined must beFalse
       h._error ! new Exception("sad panda")
     }

     e.isDefined must beTrue
     e() must be(AllHandlesDiedException)
   }
  }
}
