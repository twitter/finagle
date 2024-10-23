package com.twitter.finagle.memcached.integration

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Address
import com.twitter.finagle.Name
import com.twitter.finagle.Service
import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.Stack
import com.twitter.finagle.client.StackClient
import com.twitter.finagle.client.StdStackClient
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.dispatch.SerialClientDispatcher
import com.twitter.finagle.memcached.integration.external.InternalMemcached
import com.twitter.finagle.memcached.integration.external.TestMemcachedServer
import com.twitter.finagle.memcached.protocol.Add
import com.twitter.finagle.memcached.protocol.Cas
import com.twitter.finagle.memcached.protocol.Command
import com.twitter.finagle.memcached.protocol.Delete
import com.twitter.finagle.memcached.protocol.Get
import com.twitter.finagle.memcached.protocol.Gets
import com.twitter.finagle.memcached.protocol.Incr
import com.twitter.finagle.memcached.protocol.Set
import com.twitter.finagle.memcached.protocol.text.MessageEncoderHandler
import com.twitter.finagle.memcached.protocol.text.client.CommandToBuf
import com.twitter.finagle.netty4.Netty4Transporter
import com.twitter.finagle.netty4.encoder.BufEncoder
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.transport.TransportContext
import com.twitter.io.Buf
import com.twitter.util.Await
import com.twitter.util.Time
import io.netty.channel.ChannelPipeline
import io.netty.handler.codec.string.StringDecoder
import java.net.SocketAddress
import java.nio.charset.StandardCharsets.UTF_8
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

// Because we use our Memcached server for testing, we need to ensure that it complies to the
// Memcached protocol.
private class MemcachedServerTest extends AnyFunSuite with BeforeAndAfter {

  private[this] var realServer: TestMemcachedServer = _
  private[this] var testServer: TestMemcachedServer = _

  private[this] var realServerClient: Service[Command, String] = _
  private[this] var testServerClient: Service[Command, String] = _

  before {
    realServer = TestMemcachedServer.start().get
    testServer = InternalMemcached.start(None).get

    realServerClient = StringClient
      .apply().newService(Name.bound(Address(realServer.address)), "client")

    testServerClient = StringClient
      .apply().newService(Name.bound(Address(testServer.address)), "client")
  }

  after {
    realServer.stop()
    testServer.stop()
    Await.result(realServerClient.close(), 5.seconds)
    Await.result(testServerClient.close(), 5.seconds)
  }

  if (Option(System.getProperty("EXTERNAL_MEMCACHED_PATH")).isDefined) {
    test("NOT_FOUND") {
      assertSameResponses(Incr(Buf.Utf8("key1"), 1), "NOT_FOUND\r\n")
    }

    test("STORED") {
      assertSameResponses(
        Set(Buf.Utf8("key2"), 0, Time.epoch, Buf.Utf8("value")),
        "STORED\r\n"
      )
    }

    test("NOT_STORED") {
      assertSameResponses(Add(Buf.Utf8("key3"), 0, Time.epoch, Buf.Utf8("value")), "STORED\r\n")
      assertSameResponses(Add(Buf.Utf8("key3"), 0, Time.epoch, Buf.Utf8("value")), "NOT_STORED\r\n")
    }

    test("EXISTS") {
      assertSameResponses(
        Set(Buf.Utf8("key4"), 0, Time.epoch, Buf.Utf8("value")),
        "STORED\r\n"
      )
      assertSameResponses(Gets(Seq(Buf.Utf8("key4"))), "VALUE key4 0 5 \\d+\r\nvalue\r\nEND\r\n")

      assertSameResponses(
        Cas(Buf.Utf8("key4"), 0, Time.epoch, Buf.Utf8("value2"), Buf.Utf8("9999")),
        "EXISTS\r\n")
    }

    test("DELETED") {
      assertSameResponses(
        Set(Buf.Utf8("key5"), 0, Time.epoch, Buf.Utf8("value")),
        "STORED\r\n"
      )
      assertSameResponses(Delete(Buf.Utf8("key5")), "DELETED\r\n")
    }

    test("CLIENT_ERROR") {
      assertSameResponses(Set(Buf.Utf8("key6"), 0, Time.epoch, Buf.Utf8("value")), "STORED\r\n")
      assertSameResponses(
        Incr(Buf.Utf8("key6"), 1),
        "CLIENT_ERROR cannot increment or decrement non-numeric value\r\n")
    }

    // NO_OP will terminate the connection so can't be tested here.
    // STATS not available in the interpreter so can't be tested here.

    test("VALUES (empty)") {
      assertSameResponses(Gets(Seq(Buf.Utf8("key7"))), "END\r\n")
    }

    test("VALUES without flags without casunique") {
      assertSameResponses(Set(Buf.Utf8("key8"), 0, Time.epoch, Buf.Utf8("value")), "STORED\r\n")
      // Note how flag 0 is still returned here
      assertSameResponses(Get(Seq(Buf.Utf8("key8"))), "VALUE key8 0 5\r\nvalue\r\nEND\r\n")
    }

    test("VALUES with flags without casunique") {
      assertSameResponses(Set(Buf.Utf8("key9"), 2, Time.epoch, Buf.Utf8("value")), "STORED\r\n")
      assertSameResponses(Get(Seq(Buf.Utf8("key9"))), "VALUE key9 2 5\r\nvalue\r\nEND\r\n")
    }

    test("VALUES without flags with casunique") {
      assertSameResponses(Set(Buf.Utf8("key10"), 0, Time.epoch, Buf.Utf8("value")), "STORED\r\n")
      // Note how flag 0 is still returned here
      assertSameResponses(Gets(Seq(Buf.Utf8("key10"))), "VALUE key10 0 5 \\d+\r\nvalue\r\nEND\r\n")
    }

    test("VALUES with flags with casunique") {
      assertSameResponses(Set(Buf.Utf8("key11"), 2, Time.epoch, Buf.Utf8("value")), "STORED\r\n")
      assertSameResponses(Gets(Seq(Buf.Utf8("key11"))), "VALUE key11 2 5 \\d+\r\nvalue\r\nEND\r\n")
    }

    test("VALUES (multiple lines)") {
      assertSameResponses(Set(Buf.Utf8("key12"), 0, Time.epoch, Buf.Utf8("value")), "STORED\r\n")
      assertSameResponses(Set(Buf.Utf8("key13"), 0, Time.epoch, Buf.Utf8("value")), "STORED\r\n")

      assertSameResponses(
        Get(Seq(Buf.Utf8("key12"), Buf.Utf8("key13"))),
        "VALUE key12 0 5\r\nvalue\r\nVALUE key13 0 5\r\nvalue\r\nEND\r\n")
    }

    test("NUMBER") {
      assertSameResponses(Set(Buf.Utf8("key14"), 0, Time.epoch, Buf.Utf8("1")), "STORED\r\n")
      assertSameResponses(Incr(Buf.Utf8("key14"), 2), "3\r\n")
    }
  }

  private[this] def assertSameResponses(command: Command, response: String): Unit = {
    val testServerResponse = Await.result(testServerClient(command), 5.seconds)
    val realServerResponse = Await.result(realServerClient(command), 5.seconds)

    assert(testServerResponse.matches(response))
    assert(realServerResponse.matches(response))
  }

  private case class StringClient(
    stack: Stack[ServiceFactory[Command, String]] = StackClient.newStack,
    params: Stack.Params = Stack.Params.empty)
      extends StdStackClient[Command, String, StringClient] {

    override protected type In = Command
    override protected type Out = String
    override protected type Context = TransportContext

    object PipelineInit extends (ChannelPipeline => Unit) {
      override def apply(pipeline: ChannelPipeline): Unit = {
        pipeline.addLast("encoder", BufEncoder)
        pipeline.addLast("messageToBuf", new MessageEncoderHandler(new CommandToBuf))
        pipeline.addLast("decoder", new StringDecoder(UTF_8))
      }
    }

    protected def newDispatcher(
      transport: Transport[In, Out] { type Context <: StringClient.this.Context }
    ): Service[In, Out] = {
      new SerialClientDispatcher(transport, NullStatsReceiver)
    }

    override protected def newTransporter(
      addr: SocketAddress
    ): Transporter[Command, String, TransportContext] = {
      Netty4Transporter.raw(PipelineInit, addr, params)
    }

    override protected def copy1(
      stack: Stack[ServiceFactory[Command, String]],
      params: Stack.Params
    ): StringClient = copy(stack, params)
  }
}
