package com.twitter.finagle.thriftmux.pushsession

import com.twitter.finagle.Mux.param.OppTls
import com.twitter.finagle.exp.pushsession.{MockChannelHandle, RefPushSession}
import com.twitter.finagle.{param => fparam}
import com.twitter.finagle.mux.{Request, Response}
import com.twitter.finagle.mux.transport.{Message, OpportunisticTls}
import com.twitter.finagle.{Service, Stack, ThriftMux}
import com.twitter.finagle.mux.exp.pushsession.{MuxChannelHandle, MuxClientNegotiatingSession}
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.thriftmux.thriftscala.TestService
import com.twitter.io.{Buf, ByteReader}
import com.twitter.util.Future
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.{TFramedTransport, TMemoryBuffer}
import org.scalatest.FunSuite

class MuxDowngradingNegotiatorTest extends FunSuite {

  private class Ctx {
    private class MockMuxChannelHandle
      extends MuxChannelHandle(
        underlying = handle,
        ch = null, // only used in overridden method `sendAndForgetNow`
        params = params) {
      override def sendNowAndForget(buf: Buf): Unit = sendAndForget(buf)
    }

    lazy val statsReceiver: InMemoryStatsReceiver = new InMemoryStatsReceiver

    lazy val params: Stack.Params = ThriftMux.BaseServerParams + fparam.Stats(statsReceiver)

    lazy val muxChannelHandle: MuxChannelHandle = new MockMuxChannelHandle

    lazy val handle: MockChannelHandle[ByteReader, Buf] = new MockChannelHandle[ByteReader, Buf]()

    lazy val service: Service[Request, Response] = Service.mk { req =>
      Future.value(Response(req.body))
    }

    lazy val refSession: RefPushSession[ByteReader, Buf] = new RefPushSession[ByteReader, Buf](handle, null)

    val negotiator: MuxDowngradingNegotiator = {
      val session = new MuxDowngradingNegotiator(
        refSession = refSession,
        params = params,
        handle = muxChannelHandle,
        service = service
      )
      refSession.updateRef(session)
      session
    }
  }

  test("installs a vanilla thriftmux session if the message is a mux message") {
    new Ctx {
      val rerr = MuxClientNegotiatingSession.MarkerRerr

      refSession.receive(ByteReader(Message.encode(rerr)))

      // Should be an echo of the marker Rerr if the standard mux handshake kicked in
      val handle.SendAndForgetOne(msg) = handle.pendingWrites.dequeue()
      assert(Message.decode(msg) == MuxClientNegotiatingSession.MarkerRerr)
      assert(statsReceiver.counters(Seq("thriftmux", "connects")) == 1l)
    }
  }

  test("installs the vanilla thrift session if the message is a thrift message") {
    new Ctx {
      val thriftMsg = {
        val buffer = new TMemoryBuffer(1)
        val framed = new TFramedTransport(buffer)
        val proto = new TBinaryProtocol(framed)
        val arg = TestService.Query.Args("hi")
        TestService.Query.Args.encode(arg, proto)
        framed.flush()
        val bytes = buffer.getArray
        Buf.ByteArray.Owned(bytes)
      }

      refSession.receive(ByteReader(thriftMsg))
      handle.serialExecutor.executeAll()

      // Should be an echo of the marker Rerr if the standard mux handshake kicked in
      val handle.SendOne(msg, _) = handle.pendingWrites.dequeue()
      assert(msg == thriftMsg)
      assert(statsReceiver.counters(Seq("thriftmux", "downgraded_connects")) == 1l)
    }
  }

  test("fails to start a downgraded session if opportunistic TLS is required") {
    new Ctx {
      override lazy val params: Stack.Params =
        ThriftMux.BaseServerParams + fparam.Stats(statsReceiver) + OppTls(Some(OpportunisticTls.Required))

      val thriftMsg = {
        val buffer = new TMemoryBuffer(1)
        val framed = new TFramedTransport(buffer)
        val proto = new TBinaryProtocol(framed)
        val arg = TestService.Query.Args("hi")
        TestService.Query.Args.encode(arg, proto)
        framed.flush()
        val bytes = buffer.getArray
        Buf.ByteArray.Owned(bytes)
      }

      refSession.receive(ByteReader(thriftMsg))

      assert(handle.closedCalled)
      assert(statsReceiver.counters(Seq("tls", "upgrade", "incompatible")) == 1l)
    }
  }
}
