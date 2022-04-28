package com.twitter.finagle.partitioning

import com.twitter.util.Future
import com.twitter.finagle.Service
import com.twitter.finagle.ServiceFactory

/**
 * Represents the snapshot of the information needed to determine how to
 * carve up the request, and where to send those carved-up slices to.
 */
case class SnapPartitioner[Req, Rep, B >: PartialFunction[Any, Future[Nothing]]](
  partitionFunction: B,
  private[partitioning] val partitionMapping: Map[Int, ServiceFactory[Req, Rep]]) {

  /**
   * When given a partitionId, asynchronously returns a [[Service]] that sends
   * requests to a shard within that partition.
   *
   * @note we assume that the factory has implemented FactoryToService under the covers,
   * so the returned service does not need to be closed by the caller.
   */
  def getServiceByPartitionId(partitionId: Int): Future[Service[Req, Rep]] = {
    partitionMapping.get(partitionId) match {
      case Some(factory) => factory()
      case None =>
        Future.exception(
          new PartitionNodeManager.NoPartitionException(
            s"No partition: $partitionId found in the node manager"))
    }
  }
}

object SnapPartitioner {

  /**
   * Provides a sensible default if you don't have enough information to decide
   * how to partition.
   */
  private[partitioning] def uninitialized[
    Req,
    Rep,
    B >: PartialFunction[Any, Future[Nothing]]
  ]: SnapPartitioner[Req, Rep, B] = SnapPartitioner(PartialFunction.empty, Map.empty)
}
