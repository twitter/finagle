package com.twitter.finagle.thriftmux.pushsession

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.pushsession.RefPushSession
import com.twitter.finagle.pushsession.utils.MockChannelHandle
import com.twitter.finagle.{param => fparam}
import com.twitter.finagle.mux.Request
import com.twitter.finagle.mux.Response
import com.twitter.finagle.mux.transport.Message
import com.twitter.finagle.Service
import com.twitter.finagle.Stack
import com.twitter.finagle.ThriftMux
import com.twitter.finagle.mux.pushsession.MuxChannelHandle
import com.twitter.finagle.mux.pushsession.MuxClientNegotiatingSession
import com.twitter.finagle.mux.pushsession.SharedNegotiationStats
import com.twitter.finagle.param.OppTls
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.ssl.OpportunisticTls
import com.twitter.finagle.ssl.session.SslSessionInfo
import com.twitter.finagle.thriftmux.thriftscala.TestService
import com.twitter.io.Buf
import com.twitter.io.ByteReader
import com.twitter.util.Future
import com.twitter.util.Try
import java.security.cert.X509Certificate
import javax.net.ssl.SSLSession
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TFramedTransport
import org.apache.thrift.transport.TMemoryBuffer
import org.scalatest.funsuite.AnyFunSuite
import com.twitter.finagle.ssl.session.ServiceIdentity

class MuxDowngradingNegotiatorTest extends AnyFunSuite {

  private class Ctx {
    class MockMuxChannelHandle
        extends MuxChannelHandle(
          underlying = handle,
          ch = null, // only used in overridden method `sendAndForgetNow`
          params = params
        ) {
      override def sendNowAndForget(buf: Buf): Unit = sendAndForget(buf)
    }

    class TlsMockMuxChannelHandle extends MockMuxChannelHandle {

      var tlsActivated: Boolean = false

      override def turnOnTls(onHandshakeComplete: Try[Unit] => Unit): Unit = {
        tlsActivated = true
      }

      override val sslSessionInfo: SslSessionInfo = new SslSessionInfo {
        def usingSsl: Boolean = true
        def session: SSLSession = ???
        def sessionId: String = ???
        def cipherSuite: String = ???
        def localCertificates: Seq[X509Certificate] = ???
        def peerCertificates: Seq[X509Certificate] = ???
        override def getLocalIdentity: Option[ServiceIdentity] = None
        override def getPeerIdentity: Option[ServiceIdentity] = None
      }
    }

    lazy val statsReceiver: InMemoryStatsReceiver = new InMemoryStatsReceiver

    lazy val sharedStats = new SharedNegotiationStats(statsReceiver)

    lazy val params: Stack.Params = ThriftMux.BaseServerParams + fparam.Stats(statsReceiver)

    lazy val muxChannelHandle: MuxChannelHandle = new MockMuxChannelHandle

    lazy val handle: MockChannelHandle[ByteReader, Buf] = new MockChannelHandle[ByteReader, Buf]()

    lazy val service: Service[Request, Response] = Service.mk { req =>
      Future.value(Response(req.body))
    }

    lazy val refSession: RefPushSession[ByteReader, Buf] =
      new RefPushSession[ByteReader, Buf](handle, null)

    val negotiator: MuxDowngradingNegotiator = {
      val session = new MuxDowngradingNegotiator(
        refSession = refSession,
        params = params,
        sharedStats,
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
      assert(statsReceiver.counters(Seq("thriftmux", "connects")) == 1L)
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
      assert(statsReceiver.counters(Seq("thriftmux", "downgraded_connects")) == 1L)
    }
  }

  test("fails to start a downgraded session if opportunistic TLS is required") {
    new Ctx {
      override lazy val params: Stack.Params =
        ThriftMux.BaseServerParams + fparam.Stats(statsReceiver) +
          OppTls(Some(OpportunisticTls.Required))

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
      assert(statsReceiver.counters(Seq("mux", "tls", "upgrade", "incompatible")) == 1L)
    }
  }

  test(
    "allows downgraded session if opportunistic TLS is required AND the transport is already encrypted") {
    new Ctx {
      override lazy val params: Stack.Params =
        ThriftMux.BaseServerParams + fparam.Stats(statsReceiver) +
          OppTls(Some(OpportunisticTls.Required))

      override lazy val muxChannelHandle: TlsMockMuxChannelHandle = new TlsMockMuxChannelHandle

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

      // We don't need to initiate TLS if it's already started
      assert(!muxChannelHandle.tlsActivated)

      assert(!handle.closedCalled)
      assert(statsReceiver.counters(Seq("thriftmux", "downgraded_connects")) == 1L)
    }
  }

  test("no infinite loops if we close before the handshake is complete and fail negotiation") {
    new Ctx {
      override lazy val params: Stack.Params =
        ThriftMux.BaseServerParams + fparam.Stats(statsReceiver) +
          OppTls(Some(OpportunisticTls.Required))

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

      val closeF = refSession.close(60.seconds)
      handle.serialExecutor.executeAll()
      refSession.receive(ByteReader(thriftMsg))
      assert(handle.closedCalled)
      handle.onClosePromise.setDone()
      assert(closeF.isDefined)
      assert(handle.serialExecutor.pendingTasks == 0)
      assert(statsReceiver.counters(Seq("mux", "tls", "upgrade", "incompatible")) == 1L)
    }
  }
}
