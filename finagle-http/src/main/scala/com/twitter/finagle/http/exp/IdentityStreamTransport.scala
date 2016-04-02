package com.twitter.finagle.http.exp

import com.twitter.finagle.transport.Transport
import com.twitter.util.Future

private[http] class IdentityStreamTransport[A, B](self: Transport[A, B])
  extends StreamTransportProxy[A, B](self) {
  def write(any: A): Future[Unit] = self.write(any)
  def read(): Future[(B, Future[Unit])] = self.read().map(IdentityStreamTransport.readFn)
}

private[http] object IdentityStreamTransport {
  private[this] val _readFn: Any => (Any, Future[Unit]) = { item =>
    (item, Future.Done)
  }

  def readFn[B]: B => (B, Future[Unit]) = _readFn.asInstanceOf[B => (B, Future[Unit])]
}
