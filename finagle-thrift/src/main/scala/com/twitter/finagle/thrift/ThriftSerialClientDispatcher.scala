package com.twitter.finagle.thrift

import com.twitter.finagle.dispatch.ClientDispatcher.wrapWriteException
import com.twitter.finagle.dispatch.SerialClientDispatcher
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Future, Promise, Return, Throw}

private[finagle] class ThriftSerialClientDispatcher(
  transport: Transport[ThriftClientRequest, Array[Byte]],
  statsReceiver: StatsReceiver)
    extends SerialClientDispatcher[ThriftClientRequest, Array[Byte]](transport, statsReceiver) {

  /**
   * Thrift oneway calls are special in that they expect an empty `Array[Byte]` as a reply
   */
  override protected def dispatch(
    req: ThriftClientRequest,
    p: Promise[Array[Byte]]
  ): Future[Unit] = {
    if (!req.oneway) super.dispatch(req, p)
    else {
      write(req)
        .respond {
          case Return(_) => p.updateIfEmpty(ThriftSerialClientDispatcher.EmptyByteArray)
          case Throw(err) => wrapWriteException(err).respond(p.updateIfEmpty(_))
        }
    }

  }
}

private object ThriftSerialClientDispatcher {
  val EmptyByteArray = Return(new Array[Byte](0))
}
