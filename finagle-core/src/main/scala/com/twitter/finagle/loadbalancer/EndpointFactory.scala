package com.twitter.finagle.loadbalancer

import com.twitter.finagle._
import com.twitter.util.{Future, Time}

/**
 * A [[ServiceFactory]] which admits that it backs an endpoint by carrying around
 * the address used to create the endpoint.
 *
 * The factory also delays the creation of its implementation until it receives
 * the first request. That is, it is lazy. This is designed to allow the load
 * balancer to construct the stacks for a large collection of endpoints and
 * amortize the cost across requests. Note, this isn't related to session
 * establishment. Session establishment is lazy and on the request path already,
 * but rather creating a large number of objects per namer updates can be expensive.
 */
private class EndpointFactory[Req, Rep](
    mk: () => ServiceFactory[Req, Rep],
    val address: Address)
  extends ServiceFactory[Req, Rep] {

    // access to this state is mediated by the lock on `this`.
    private[this] var underlying: ServiceFactory[Req, Rep] = null
    private[this] var isClosed = false

    def apply(conn: ClientConnection): Future[Service[Req, Rep]] = {
      synchronized {
        if (isClosed) return Future.exception(new ServiceClosedException)
        if (underlying == null) underlying = mk()
      }
      underlying(conn)
    }

    def close(deadline: Time): Future[Unit] = synchronized {
      isClosed = true
      if (underlying == null) Future.Done
      else underlying.close(deadline)
    }

    override def status: Status = synchronized {
      if (underlying != null) underlying.status else {
        if (!isClosed) Status.Open
        else Status.Closed
      }
    }

    /**
     * Returns the underlying [[ServiceFactory]] if it is materialized
     * otherwise None.
     */
    def self: Option[ServiceFactory[Req, Rep]] = synchronized { Option(underlying) }

    override def toString: String = address.toString
  }