package com.twitter.finagle.redis

import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.ServiceFactory

private[redis] object ClusterClient {

  /**
   * Construct a client client from a single host.
   * @param host a String of host:port combination.
   */
  def apply(host: String): ClusterClient = {
    ClusterClient(com.twitter.finagle.Redis.newClient(host))
  }

  /**
   * Construct a cluster client from a single Service.
   */
  def apply(raw: ServiceFactory[Command, Reply]): ClusterClient =
    new ClusterClient(raw)
}

private[redis] class ClusterClient(factory: ServiceFactory[Command, Reply])
    extends BaseClient(factory)
    with BasicServerCommands
    with KeyCommands
    with StringCommands
    with ClusterCommands
