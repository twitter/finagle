package com.twitter.finagle.mysql.transport

import com.twitter.finagle.mysql.QuitRequest
import com.twitter.finagle.transport.{Transport, TransportProxy}
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util.{Future, Time}

/**
 * A `MysqlTransport` is a representation of a stream of MySQL packets that
 * may be read from and written to asynchronously. It is also capable of
 * sending a proper `COM_QUIT` on close.
 */
private[mysql] final class MysqlTransport(_self: Transport[Packet, Packet])
    extends TransportProxy(_self) {

  def read(): Future[Packet] = _self.read()

  def write(input: Packet): Future[Unit] = _self.write(input)

  override def close(deadline: Time): Future[Unit] =
    write(QuitRequest.toPacket)
      .by(DefaultTimer, deadline)
      // the `write(...).by` might throw a TimeoutException,
      // which isn't material to us since we are going to
      // close the underlying transport regardless, adding
      // a transform() here to swallow the exception.
      .transform(_ => Future.Unit)
      .ensure(_self.close(deadline))
}
