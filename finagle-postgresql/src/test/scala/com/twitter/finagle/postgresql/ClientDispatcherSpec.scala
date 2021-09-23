package com.twitter.finagle.postgresql

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.PostgreSql
import com.twitter.finagle.Status
import com.twitter.finagle.ssl.session.NullSslSessionInfo
import com.twitter.finagle.transport.SimpleTransportContext
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.transport.TransportContext
import com.twitter.util.Await
import com.twitter.util.Future
import com.twitter.util.Promise
import com.twitter.util.Return
import com.twitter.util.Time
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec
import java.net.SocketAddress

object ClientDispatcherSpec {
  final class MockPqTransport(
    steps: Seq[PartialFunction[FrontendMessage, Seq[BackendMessage]]])
      extends Transport[FrontendMessage, BackendMessage] {
    override type Context = TransportContext

    private[this] val closep = new Promise[Throwable]
    private[this] val iter = steps.iterator
    private[this] var nextReads: Iterator[BackendMessage] = _

    def write(msg: FrontendMessage): Future[Unit] =
      iter.next().lift.apply(msg) match {
        case Some(next) =>
          nextReads = next.iterator
          Future.Done
        case None =>
          val err = new NoSuchElementException(s"expected a transition for input $msg")
          closep.setException(err)
          Future.exception(err)
      }

    def read(): Future[BackendMessage] = {
      Future.value(nextReads.next())
    }

    def status: Status = if (closep.isDefined) Status.Closed else Status.Open

    def close(deadline: Time): Future[Unit] = {
      val ex = new Exception("QueueTransport is now closed")
      closep.updateIfEmpty(Return(ex))
      Future.Done
    }

    val onClose: Future[Throwable] = closep
    val context: TransportContext =
      new SimpleTransportContext(new SocketAddress {}, new SocketAddress {}, NullSslSessionInfo)
  }

  private def expect(
    frontendMessage: FrontendMessage,
    backendMessages: BackendMessage*
  ): PartialFunction[FrontendMessage, Seq[BackendMessage]] = {
    case m if m == frontendMessage => backendMessages
  }
}

class ClientDispatcherSpec extends AnyWordSpec with Matchers {
  import ClientDispatcherSpec._

  "ClientDispatcher" should {
    "send a statement timeout when set" in {
      val stackParams = PostgreSql.defaultParams + Params.StatementTimeout(1.second)
      val expects = Seq(
        expect(
          FrontendMessage
            .StartupMessage(user = "postgres", params = Map("statement_timeout" -> "1000")),
          BackendMessage.AuthenticationOk,
          BackendMessage.ReadyForQuery(BackendMessage.NoTx)
        ),
        expect(FrontendMessage.Sync, BackendMessage.ReadyForQuery(BackendMessage.NoTx))
      )

      val transport = new MockPqTransport(expects)
      val dispatcher = new ClientDispatcher(transport, stackParams)

      val fut = dispatcher.apply(Request.Sync)
      Await.result(fut)
    }

    "send a session defaults when set" in {
      val params = Map("a" -> "b", "c" -> "d")
      val stackParams = PostgreSql.defaultParams + Params.SessionDefaults(params)
      val expects = Seq(
        expect(
          FrontendMessage.StartupMessage(user = "postgres", params = params),
          BackendMessage.AuthenticationOk,
          BackendMessage.ReadyForQuery(BackendMessage.NoTx)),
        expect(FrontendMessage.Sync, BackendMessage.ReadyForQuery(BackendMessage.NoTx))
      )

      val transport = new MockPqTransport(expects)
      val dispatcher = new ClientDispatcher(transport, stackParams)

      val fut = dispatcher.apply(Request.Sync)
      Await.result(fut)
    }

    "send connection initialization sql when set" in {
      val stackParams = PostgreSql.defaultParams + Params.ConnectionInitializationCommands(
        Seq("set x = y", "set a = b"))
      val expects = Seq(
        expect(
          FrontendMessage.StartupMessage(user = "postgres"),
          BackendMessage.AuthenticationOk,
          BackendMessage.ReadyForQuery(BackendMessage.NoTx)),
        expect(
          FrontendMessage.Query("set x = y;\nset a = b"),
          BackendMessage.CommandComplete(BackendMessage.CommandTag.Other("SET")),
          BackendMessage.ReadyForQuery(BackendMessage.NoTx)
        ),
        expect(FrontendMessage.Sync, BackendMessage.ReadyForQuery(BackendMessage.NoTx))
      )

      val transport = new MockPqTransport(expects)
      val dispatcher = new ClientDispatcher(transport, stackParams)

      val fut = dispatcher.apply(Request.Sync)
      Await.result(fut)
    }
  }
}
