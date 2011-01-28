package com.twitter.finagle.pool

import com.twitter.util.Future

import com.twitter.finagle.{Service, ServiceClosedException}

/**
 * A pool of services.
 */
class PoolingService[Req, Rep, S <: Service[Req, Rep]](pool: DrainablePool[S])
  extends Service[Req, Rep]
{
  private[this] var isOpen = true

  def apply(request: Req): Future[Rep] = synchronized {
    if (!isOpen) return Future.exception(new ServiceClosedException)

    pool.reserve() flatMap { service =>
      service(request) ensure { pool.release(service) }
    }
  }

  // TODO: include measures of underlying health / pool occupancy?
  override def isAvailable = synchronized { isOpen }

  override def close() = synchronized {
    isOpen = false
    pool.drain()
  }
}

