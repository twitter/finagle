package com.twitter.finagle.postgresql

import com.twitter.finagle.Stack
import com.twitter.finagle.dispatch.ClientDispatcher.wrapWriteException
import com.twitter.finagle.dispatch.GenSerialClientDispatcher
import com.twitter.finagle.param.Stats
import com.twitter.finagle.postgresql.Params.Credentials
import com.twitter.finagle.postgresql.Params.Database
import com.twitter.finagle.postgresql.machine.HandshakeMachine
import com.twitter.finagle.postgresql.machine.MachineRunner
import com.twitter.finagle.postgresql.transport.Packet
import com.twitter.finagle.transport.Transport
import com.twitter.util.Future
import com.twitter.util.Promise
import com.twitter.util.Return
import com.twitter.util.Throw
import com.twitter.util.Try

class ClientDispatcher(
  transport: Transport[Packet, Packet],
  params: Stack.Params,
) extends GenSerialClientDispatcher[Request, Response, Packet, Packet](
  transport,
  params[Stats].statsReceiver
) {

  private[this] val tryReadTheTransport: Try[Unit] => Future[Packet] = {
    case Return(_) => transport.read()
    case Throw(exc) => wrapWriteException(exc)
  }

  val connect =
    MachineRunner(transport, HandshakeMachine(params[Credentials], params[Database])).run

  override def apply(req: Request): Future[Response] =
    connect.unit before { super.apply(req) }

  def exchange(req: Messages.FrontendMessage): Future[Messages.BackendMessage] =
    transport
      .write(req.write)
      .transform(tryReadTheTransport)
      .map(rep => Messages.BackendMessage.read(rep))

  // TODO: based on the Request, we start a state machine that does the backend and forth
  //   with the backend and eventually prodices a Response.
  override protected def dispatch(req: Request, p: Promise[Response]): Future[Unit] =
    req match {
      case Sync =>
        val resp = exchange(Messages.Sync)
        p.become(resp.map(BackendResponse))
        resp.unit
    }
}
