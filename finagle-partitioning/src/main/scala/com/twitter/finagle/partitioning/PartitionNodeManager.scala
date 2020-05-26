package com.twitter.finagle.partitioning

import com.twitter.finagle._
import com.twitter.finagle.addr.WeightedAddress
import com.twitter.finagle.loadbalancer.TrafficDistributor._
import com.twitter.finagle.loadbalancer.LoadBalancerFactory
import com.twitter.finagle.param.{Label, Logger}
import com.twitter.finagle.partitioning.zk.ZkMetadata
import com.twitter.logging.{HasLogLevel, Level}
import com.twitter.util._
import java.util.concurrent.atomic.AtomicReference
import scala.util.control.NonFatal

private[finagle] object PartitionNodeManager {

  /**
   * A ServiceFactory that hold an updatable collection of address to configure the
   * its LoadBalancerFactory.
   */
  private class CachedServiceFactory[Req, Rep](
    val endpoints: Var[Addr] with Updatable[Addr] with Extractable[Addr],
    params: Stack.Params,
    underlying: Stack[ServiceFactory[Req, Rep]])
      extends Closable {

    val factory: ServiceFactory[Req, Rep] = {
      val paramsWithLB = params + LoadBalancerFactory.Dest(endpoints)
      underlying.make(paramsWithLB)
    }

    def close(deadline: Time): Future[Unit] = factory.close(deadline)
  }

  /**
   * The given partition Id cannot be found in the Partition node map.
   */
  final class NoPartitionException(
    message: String,
    val flags: Long = FailureFlags.Empty)
      extends Exception(message)
      with FailureFlags[NoPartitionException]
      with HasLogLevel {
    def logLevel: Level = Level.ERROR

    protected def copyWithFlags(flags: Long): NoPartitionException =
      new NoPartitionException(message, flags)
  }

  /**
   * Cannot retrieve the shardId information from [[ZkMetadata]].
   */
  final class NoShardIdException(
    message: String,
    val flags: Long = FailureFlags.Empty)
      extends Exception(message)
      with FailureFlags[NoShardIdException]
      with HasLogLevel {
    def logLevel: Level = Level.ERROR

    protected def copyWithFlags(flags: Long): NoShardIdException =
      new NoShardIdException(message, flags)
  }
}

/**
 * This is a helper class for managing partitioned service nodes when the client is
 * configured with a custom partitioning strategy. This is used in one implemented
 * [[PartitioningService]] to construct client stack endpoints for each logical
 * partition, retrieved by partition id. The fundamental problem is that service
 * discovery gives us undifferentiated hosts, which we want to aggregate into
 * partitions. So we need to take a reverse lookup and group by the partition into
 * collections of addresses, which we can loadbalance over.
 *
 * This node manager maintains a map of (partitionId, Future[Service]), partitions are
 * logical partitions can have one or more than one host. [[PartitionNodeManager]] listens
 * to client's [[LoadBalancerFactory.Dest]] (collection of addrs) changes then updates the map.
 *
 * @note  Node manager tracks all addresses as weighted addresses, which means a
 *        weight change for a given node will be considered a node restart. This
 *        way implementations can adjust their partitions if weight is a factor
 *        in partitioning.
 *
 * @param underlying          Finagle client stack
 *
 * @param getLogicalPartition Getting the logical partition identifier from a host identifier.
 *                            Reverse lookup. Indicates which logical partition a physical host
 *                            belongs to, this is provided by client configuration when needed,
 *                            multiple hosts can belong to the same partition, for example:
 *                            {{{
 *                                val getLogicalPartition: Int => Int = {
 *                                  case a if Range(0, 10).contains(a) => 0
 *                                  case b if Range(10, 20).contains(b) => 1
 *                                  case c if Range(20, 30).contains(c) => 2
 *                                  case _ => throw ...
 *                                }
 *                            }}}
 *                            if not provided, each host is a partition.
 *                            Host identifiers are derived from [[ZkMetadata]] shardId, logical
 *                            partition identifiers are defined by users in [[PartitioningStrategy]]
 *
 * @param params              Configured Finagle client params
 */
private[finagle] class PartitionNodeManager[Req, Rep](
  underlying: Stack[ServiceFactory[Req, Rep]],
  getLogicalPartition: Int => Int,
  params: Stack.Params)
    extends Closable { self =>

  import PartitionNodeManager._

  private[this] val logger = params[Logger].log
  private[this] val label = params[Label].label

  private[this] val partitionServiceNodes =
    new AtomicReference[Map[Int, ServiceFactory[Req, Rep]]]()

  // Keep track of addresses in the current set that already have associate instances
  private[this] val destActivity = varAddrToActivity(params[LoadBalancerFactory.Dest].va, label)

  // listen to the WeightedAddress changes, transform the changes to a stream of
  // partition id (includes errors) to [[CachedServiceFactory]].
  private[this] val partitionAddressChanges: Event[
    Activity.State[Map[Try[Int], CachedServiceFactory[Req, Rep]]]
  ] = {
    val cachedServiceFactoryDiffOps = new DiffOps[Address, CachedServiceFactory[Req, Rep]] {
      def remove(factory: CachedServiceFactory[Req, Rep]): Unit = factory.close()

      def add(addresses: Set[Address]): CachedServiceFactory[Req, Rep] =
        new CachedServiceFactory(Var(Addr.Bound(addresses)), params, underlying)

      def update(
        addresses: Set[Address],
        factory: CachedServiceFactory[Req, Rep]
      ): CachedServiceFactory[Req, Rep] = {
        factory.endpoints.update(Addr.Bound(addresses))
        factory
      }
    }

    val getShardIdFromAddress: Address => Try[Int] = {
      case WeightedAddress(Address.Inet(_, metadata), _) =>
        ZkMetadata.fromAddrMetadata(metadata).flatMap(_.shardId) match {
          case Some(id) =>
            try {
              val partitionId = getLogicalPartition(id)
              Return(partitionId)
            } catch {
              case NonFatal(e) =>
                logger.log(Level.ERROR, "getLogicalPartition failed with: ", e)
                Throw(e)
            }
          case None =>
            val ex = new NoShardIdException(s"cannot get shardId from $metadata")
            logger.log(Level.ERROR, "getLogicalPartition failed with: ", ex)
            Throw(ex)
        }
    }

    val init = Map.empty[Try[Int], CachedServiceFactory[Req, Rep]]
    safelyScanLeft(init, destActivity.states) { (partitionNodes, activeSet) =>
      updatePartitionMap[Try[Int], CachedServiceFactory[Req, Rep], Address](
        partitionNodes,
        activeSet,
        getShardIdFromAddress,
        cachedServiceFactoryDiffOps
      )
    }
  }

  // Transform the stream of [[CachedServiceFactory]] to ServiceFactory and filter out
  // the failed partition id
  private[this] val partitionNodesChange: Event[Map[Int, ServiceFactory[Req, Rep]]] = {
    val init = Map.empty[Int, ServiceFactory[Req, Rep]]
    partitionAddressChanges
      .foldLeft(init) {
        case (_, Activity.Ok(partitions)) =>
          // this could possibly be an empty update if getLogicalPartition returns all Throws
          partitions.filter(_._1.isReturn).map {
            case (key, sf) => (key.get() -> sf.factory)
          }
        case (staleState, _) => staleState
      }.filter(_.nonEmpty)
  }

  private[this] val nodeWatcher: Closable =
    partitionNodesChange.register(Witness(partitionServiceNodes))

  /**
   * Returns a Future of [[Service]] which maps to the given partitionId.
   *
   * Note: The caller is responsible for relinquishing the use of the returned [[Service]
   * by calling [[Service#close()]]. Close this node manager will close all underlying services.
   *
   * @param partitionId logical partition id
   */
  def getServiceByPartitionId(partitionId: Int): Future[Service[Req, Rep]] = {
    partitionServiceNodes.get.get(partitionId) match {
      case Some(factory) => factory()
      case None =>
        Future.exception(
          new NoPartitionException(s"No partition: $partitionId found in the node manager"))
    }
  }

  /**
   * When close the node manager, all underlying services are closed.
   */
  def close(deadline: Time): Future[Unit] = self.synchronized {
    nodeWatcher.close(deadline)
    Closable.all(partitionServiceNodes.get.values.toSeq: _*).close(deadline)
  }
}
