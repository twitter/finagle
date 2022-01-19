package com.twitter.finagle.postgresql.transport

import com.twitter.finagle.Stack
import com.twitter.finagle.param
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.postgresql.Client.Expect
import com.twitter.finagle.postgresql.Params.Credentials
import com.twitter.finagle.postgresql.Params.Database
import com.twitter.finagle.postgresql.Params.SessionDefaults
import com.twitter.finagle.postgresql.Params.StatementTimeout
import com.twitter.finagle.postgresql.Response.ConnectionParameters
import com.twitter.finagle.postgresql.machine.HandshakeMachine
import com.twitter.finagle.postgresql.machine.Runner
import com.twitter.finagle.postgresql.machine.SimpleQueryMachine
import com.twitter.finagle.postgresql.BackendMessage
import com.twitter.finagle.postgresql.FrontendMessage
import com.twitter.finagle.postgresql.Params
import com.twitter.finagle.postgresql.Response
import com.twitter.finagle.transport.Transport
import com.twitter.io.Reader
import com.twitter.util.Future
import com.twitter.util.Promise

private[postgresql] object ConnectionHandshaker {

  private def runInitializationCommands(params: Stack.Params, runner: Runner): Future[Unit] =
    params[Params.ConnectionInitializationCommands].commands.toList match {
      case Nil =>
        Future.Done
      case initCmds =>
        val p = new Promise[Response]()

        val machineComplete = runner.dispatch(
          new SimpleQueryMachine(initCmds.mkString(";\n"), ConnectionParameters.empty),
          p
        )

        val queryResponses = p
          .flatMap(Expect.SimpleQueryResponse)
          .flatMap(resp => Reader.readAllItems(resp.responses.map(Expect.Command)))
          .flatMap(l => Future.collect(l))

        machineComplete.before(queryResponses.unit)
    }

  def apply(
    transport: Transport[FrontendMessage, BackendMessage],
    params: Stack.Params
  ): Future[Response.ConnectionParameters] = {

    val Transporter.ConnectTimeout(connectTimeout) = params[Transporter.ConnectTimeout]
    val param.Timer(timer) = params[com.twitter.finagle.param.Timer]
    val runner = new Runner(transport)
    val connectionParameters = Promise[Response.ConnectionParameters]()

    runner
      .dispatch(
        HandshakeMachine(
          params[Credentials],
          params[Database],
          params[StatementTimeout],
          params[SessionDefaults]),
        connectionParameters)
      .before {
        runInitializationCommands(params, runner)
      }
      .before {
        // technically `connectionParameters` is always satisfied here by the time dispatch completes, however,
        // using `before` here will allow the timeout to propegate through the entire handshake.
        connectionParameters
      }
      .raiseWithin(connectTimeout)(timer)
  }
}
