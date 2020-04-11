package com.twitter.finagle

import java.net.SocketAddress

import com.twitter.finagle.client.StackClient
import com.twitter.finagle.client.StdStackClient
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.dispatch.GenSerialClientDispatcher
import com.twitter.finagle.dispatch.GenSerialClientDispatcher.wrapWriteException
import com.twitter.finagle.param.Stats
import com.twitter.finagle.postgresql.Messages.BackendMessage
import com.twitter.finagle.postgresql.Messages.FrontendMessage
import com.twitter.finagle.postgresql.Params
import com.twitter.finagle.postgresql.PgsqlTransporter
import com.twitter.finagle.postgresql.transport.Packet
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.transport.TransportContext
import com.twitter.util.Future
import com.twitter.util.Promise
import com.twitter.util.Return
import com.twitter.util.Throw
import com.twitter.util.Try

object PostgreSQL {

  val defaultStack: Stack[ServiceFactory[FrontendMessage, BackendMessage]] = StackClient.newStack[FrontendMessage, BackendMessage]
  val defaultParams: Stack.Params = StackClient.defaultParams

  case class Client(
    stack:  Stack[ServiceFactory[FrontendMessage, BackendMessage]] = defaultStack,
    params: Stack.Params = defaultParams
  ) extends StdStackClient[FrontendMessage, BackendMessage, Client] {

    override type In = Packet
    override type Out = Packet
    override type Context = TransportContext

    def withCredentials(u: String, p: Option[String]): Client =
      configured(Params.Credentials(u, p))

    def withDatabase(db: String): Client =
      configured(Params.Database(Some(db)))

    override protected def newTransporter(addr: SocketAddress): Transporter[Packet, Packet, TransportContext] = {
      new PgsqlTransporter(addr, params)
    }

    override protected def newDispatcher(trans: Transport[Packet, Packet] {
      type Context <: TransportContext
    }
    ): Service[FrontendMessage, BackendMessage] = new GenSerialClientDispatcher[FrontendMessage, BackendMessage, Packet, Packet](trans, params[Stats].statsReceiver) {

      private[this] val tryReadTheTransport: Try[Unit] => Future[Packet] = {
        case Return(_) => trans.read()
        case Throw(exc) => wrapWriteException(exc)
      }

      // TODO: this isn't how we're supposed to dispatch since PgSQL isn't req/res based
      override protected def dispatch(req: FrontendMessage, p: Promise[BackendMessage]): Future[Unit] = {
        trans
          .write(req.write)
          .transform(tryReadTheTransport)
          .map(rep => BackendMessage.read(rep))
          .respond(rep => p.updateIfEmpty(rep))
          .unit
      }
    }

    override protected def copy1(stack: Stack[ServiceFactory[FrontendMessage, BackendMessage]], params: Stack.Params): Client {
      type In = Packet
      type Out = Packet
    } = copy(stack, params)
  }

}
