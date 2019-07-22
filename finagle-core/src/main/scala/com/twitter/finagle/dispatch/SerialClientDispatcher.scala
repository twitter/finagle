package com.twitter.finagle.dispatch

import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Future, Promise, Return, Throw, Try}

/**
 * @param statsReceiver typically scoped to `clientName/dispatcher`
 */
class SerialClientDispatcher[Req, Rep](trans: Transport[Req, Rep], statsReceiver: StatsReceiver)
    extends GenSerialClientDispatcher[Req, Rep, Req, Rep](trans, statsReceiver) {

  import ClientDispatcher.wrapWriteException

  private[this] val tryReadTheTransport: Try[Unit] => Future[Rep] = {
    case Return(_) => trans.read()
    case Throw(exc) => wrapWriteException(exc)
  }

  protected def dispatch(req: Req, p: Promise[Rep]): Future[Unit] =
    trans
      .write(req)
      .transform(tryReadTheTransport)
      .respond(rep => p.updateIfEmpty(rep))
      .unit

  protected def write(req: Req): Future[Unit] = trans.write(req)
}
