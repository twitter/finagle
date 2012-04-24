package com.twitter.finagle

import com.twitter.util.Future
import com.twitter.finagle.transport.TransportFactory

package object dispatch {
  type ClientDispatcher[Req, Rep] = Req => Future[Rep]
  type ClientDispatcherFactory[Req, Rep] = TransportFactory => ClientDispatcher[Req, Rep]
  type ServerDispatcherFactory[Req, Rep] = (TransportFactory, Service[Req, Rep]) => ServerDispatcher
}
