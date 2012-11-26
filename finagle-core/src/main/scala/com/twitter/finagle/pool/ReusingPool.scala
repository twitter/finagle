package com.twitter.finagle.pool

import com.twitter.finagle.{ServiceFactory, ServiceProxy, ServiceFactoryProxy, ClientConnection}

class ReusingPool[Req, Rep](underlying: ServiceFactory[Req, Rep]) 
    extends ServiceFactoryProxy[Req, Rep](underlying) {
  private[this] val f = underlying() map { service => new ServiceProxy(service) {
      override def release() = ()  // No-op
    }
  }
  // TODO: discard connection when disconnected


  override def apply(conn: ClientConnection) = f
}
