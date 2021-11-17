package com.twitter.finagle.http

import com.twitter.finagle.transport.Transport
import com.twitter.util.Future

private[finagle] class IdentityStreamTransport[A, B](self: Transport[A, B])
    extends StreamTransportProxy[A, B](self) {
  def write(any: A): Future[Unit] = self.write(any)
  def read(): Future[Multi[B]] = self.read().map(IdentityStreamTransport.readFn)
}

private[http] object IdentityStreamTransport {
  private[this] val _readFn: Any => Multi[Any] = { item => Multi(item, Future.Done) }

  def readFn[B]: B => Multi[B] = _readFn.asInstanceOf[B => Multi[B]]
}
