package com.twitter.finagle

import com.twitter.util.{Future, Time}

/**
 * A [[ServiceFactory]] that proxies all calls to another
 * ServiceFactory.  This can be useful if you want to modify
 * an existing `ServiceFactory`.
 */
abstract class ServiceFactoryProxy[-Req, +Rep](_self: ServiceFactory[Req, Rep])
    extends ServiceFactory[Req, Rep] {
  def self: ServiceFactory[Req, Rep] = _self

  def apply(conn: ClientConnection): Future[Service[Req, Rep]] = self(conn)
  def close(deadline: Time): Future[Unit] = self.close(deadline)
  override def toString: String = s"${getClass.getName}(${self.toString})"
  override def status: Status = self.status
}
