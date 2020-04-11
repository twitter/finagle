package com.twitter.finagle.postgresql

import com.twitter.finagle.Stack
import com.twitter.finagle.dispatch.ClientDispatcher.wrapWriteException
import com.twitter.finagle.dispatch.GenSerialClientDispatcher
import com.twitter.finagle.param.Stats
import com.twitter.finagle.postgresql.Messages.BackendMessage
import com.twitter.finagle.postgresql.Messages.FrontendMessage
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
) extends GenSerialClientDispatcher[FrontendMessage, BackendMessage, Packet, Packet](
  transport,
  params[Stats].statsReceiver
) {

  private[this] val tryReadTheTransport: Try[Unit] => Future[Packet] = {
    case Return(_) => transport.read()
    case Throw(exc) => wrapWriteException(exc)
  }

  // TODO: we want to replace this with the connection state machine
  val connect = {
    val handshake = Handshake(params, transport)
    handshake
      .startup()
      .ensure(println) // TODO: logging
  }

  override def apply(req: FrontendMessage): Future[BackendMessage] =
    connect.unit before { super.apply(req) }

  // TODO: this isn't how we're supposed to dispatch since PgSQL isn't req/res based
  override protected def dispatch(req: FrontendMessage, p: Promise[BackendMessage]): Future[Unit] = {
    transport
      .write(req.write)
      .transform(tryReadTheTransport)
      .map(rep => BackendMessage.read(rep))
      .respond(rep => p.updateIfEmpty(rep))
      .unit
  }
}
