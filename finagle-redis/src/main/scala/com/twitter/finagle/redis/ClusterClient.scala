package com.twitter.finagle.redis

import com.twitter.finagle.{ClientConnection, Redis, Service, ServiceFactory, ServiceFactoryProxy}
import com.twitter.finagle.redis.protocol._
import com.twitter.io.{Buf, ByteReader}
import com.twitter.util.{Closable, Future, Time}
import com.twitter.hashing.Hashable

object ClusterClient {
  /**
   * Construct a single cluster client from a single bootstrap host.
   * @param host a String of host:port combination.
   */
  def apply(host: String): ClusterClient =
    ClusterClient(Redis.newClient(host))

  /**
   * Construct a single cluster client from a service factory
   * @param hosts a sequence of String of host:port combination.
   */
  def apply(raw: ServiceFactory[Command, Reply]): ClusterClient = {
    new ClusterClient(raw)
  }
}

trait ClusterClientCommands
extends ClusterCommands
with NormalCommands
with Closable {
  self: BaseClient =>

  override def select(index: Int): Future[Unit] =
    Future.exception(new NotImplementedError("Not supported by Redis Cluster"))
}

class ClusterClient(factory: ServiceFactory[Command, Reply])
  extends BaseSingleClient(factory)
  with ClusterClientCommands {

  private val Moved = "MOVED"
  private val Ask = "ASK"
  private val TryAgain = "TRYAGAIN"

  private[redis] override def doRequest[T](
    cmd: Command
  )(handler: PartialFunction[Reply, Future[T]]): Future[T] = {
    super.doRequest(cmd)(handler)
      .rescue {
        case ServerError(msg) if msg.startsWith(Moved) =>
          val Array(_, slotId, host) = msg.split(" ")
          Future.exception(ClusterMovedError(slotId.toInt, host))

        case ServerError(msg) if msg.startsWith(Ask) =>
          val Array(_, slotId, host) = msg.split(" ")
          Future.exception(ClusterAskError(slotId.toInt, host))

        case ServerError(msg) if msg.startsWith(TryAgain) =>
          Future.exception(ClusterTryAgainError())
      }
  }
}

