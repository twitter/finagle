package com.twitter.finagle.postgresql

import com.twitter.finagle.postgresql.mock.MockPgTransport
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec
import com.twitter.finagle.PostgreSql
import com.twitter.finagle.postgresql
import com.twitter.conversions.DurationOps._
import com.twitter.finagle.postgresql.transport.ConnectionHandshaker
import com.twitter.util.Await

class ConnectionHandshakerSpec extends AnyWordSpec with Matchers with PropertiesSpec {
  import MockPgTransport._

  "ConnectionHandshaker" should {
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

      val transport = new MockPgTransport(expects)
      val fut = ConnectionHandshaker(transport, stackParams)

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

      val transport = new MockPgTransport(expects)
      val fut = postgresql.transport.ConnectionHandshaker(transport, stackParams)

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

      val transport = new MockPgTransport(expects)
      val fut = postgresql.transport.ConnectionHandshaker(transport, stackParams)

      Await.result(fut)
    }
  }
}
