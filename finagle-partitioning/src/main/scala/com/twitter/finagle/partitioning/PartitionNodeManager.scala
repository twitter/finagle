package com.twitter.finagle.partitioning

import com.twitter.finagle._
import com.twitter.finagle.addr.WeightedAddress
import com.twitter.finagle.loadbalancer.EndpointFactory
import com.twitter.finagle.loadbalancer.LoadBalancerFactory
import com.twitter.finagle.loadbalancer.NotClosableEndpointFactoryProxy
import com.twitter.finagle.loadbalancer.TrafficDistributor._
import com.twitter.finagle.loadbalancer.distributor.AddrLifecycle._
import com.twitter.finagle.param.Label
import com.twitter.finagle.param.Stats
import com.twitter.finagle.partitioning.zk.ZkMetadata
import com.twitter.finagle.stats.Verbosity
import com.twitter.logging.HasLogLevel
import com.twitter.logging.Level
import com.twitter.util._
import java.util.concurrent.atomic.AtomicReference
import scala.util.control.NonFatal

private[finagle] object PartitionNodeManager {

  private val logger = logging.Logger.getLogger(classOf[PartitionNodeManager[_, _, _, _]])

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
 * @param observable          The state that determines the sharding scheme we use.  The updated values
 *                            are used by `getPartitionFunctionPerState` and `getLogicalPartitionPerState`
 *                            as soon as they're updated.
 *
 * @param getPartitionFunctionPerState When given a state, gets the partitioning function, which can be used
 *                            to describe which partitions a request should be subdivided into, and how a request
 *                            should be sliced and diced for each partition.
 *                            Note that this function must be referentially transparent.
 *
 * @param getLogicalPartitionPerState When given a state, gets the logical partition identifiers
 *                            from a host identifier.
 *                            Reverse lookup. Indicates which logical partitions a physical host
 *                            belongs to, this is provided by client configuration when needed,
 *                            multiple hosts can belong to the same partition, one host can belong
 *                            to multiple hosts, for example:
 *                            {{{
 *                                val getLogicalPartition: Int => Seq[Int] = {
 *                                  case a if Range(0, 10).contains(a) => Seq(0, 1)
 *                                  case b if Range(10, 20).contains(b) => Seq(1)
 *                                  case c if Range(20, 30).contains(c) => Seq(2)
 *                                  case _ => throw ...
 *                                }
 *                            }}}
 *                            if not provided, each host is a partition.
 *                            Host identifiers are derived from [[ZkMetadata]] shardId, logical
 *                            partition identifiers are defined by users in [[PartitioningStrategy]]
 *                            Note that this function must be referentially transparent.
 *
 * @param params              Configured Finagle client params
 *
 * @tparam Req the request type
 *
 * @tparam Rep the response type
 *
 * @tparam A   parameterizes the observable.  this is the type of the state that determines how
 *             requests get partitioned.
 *
 * @tparam B   the type of a partitioning function that will be snapshotted given a new state of
 *             type B.
 */
private[finagle] class PartitionNodeManager[
  Req,
  Rep,
  A,
  B >: PartialFunction[Any, Future[Nothing]]
](
  underlying: Stack[ServiceFactory[Req, Rep]],
  observable: Activity[A],
  getPartitionFunctionPerState: A => B,
  getLogicalPartitionPerState: A => Int => Seq[Int],
  params: Stack.Params)
    extends Closable { self =>

  import PartitionNodeManager._

  private[this] val label = params[Label].label
  private[this] val statsReceiver = {
    val stats = params[Stats].statsReceiver
    stats.scope("partitioner")
  }

  // we initialize this immediately because we filter the event that keeps
  // track of partitions later.  if some of them are empty, this will be null.
  // it should at least be shaped right, so that the exceptions we get are more
  // useful than NPEs
  // TODO we should return a DelayingServiceFactory until `observable` is no
  // longer pending
  private[this] val partitionServiceNodes = new AtomicReference(
    SnapPartitioner.uninitialized[Req, Rep, B]
  )

  private[this] val addressedFactories: AtomicReference[Try[Set[EndpointFactory[Req, Rep]]]] =
    new AtomicReference(Return(Set.empty))

  private[this] val partitionerMetrics =
    statsReceiver.addGauge("nodes") { partitionServiceNodes.get.partitionMapping.size }

  private[this] val partitionAddressChangesCounter =
    statsReceiver.counter("partition_updates", Verbosity.Debug)
  private[this] val snapUpdates = statsReceiver.counter("snap_updates", Verbosity.Debug)

  // Lazy as this metric is not particularly helpful unless there are failures.
  private[this] lazy val updateFailures = statsReceiver.counter("update_failures")

  // Keep track of addresses in the current set that already have associate instances
  private[this] val destActivity = varAddrToActivity(params[LoadBalancerFactory.Dest].va, label)

  private[this] val addressedEndpoints = Activity {
    val newEndpointStk =
      underlying.dropWhile(_.head.role != LoadBalancerFactory.role).tailOption.get
    weightEndpoints(
      destActivity,
      LoadBalancerFactory.newEndpointFn(params, newEndpointStk),
      !params[LoadBalancerFactory.EnableProbation].enable
    )
  }

  private[this] def getShardIdFromFactory(
    state: A,
    factory: EndpointFactory[Req, Rep]
  ): Seq[Try[Int]] = {
    val metadata = factory.address match {
      case WeightedAddress(Address.Inet(_, metadata), _) => metadata
      case Address.ServiceFactory(_, metadata) => metadata
    }
    ZkMetadata.fromAddrMetadata(metadata).flatMap(_.shardId) match {
      case Some(id) =>
        try {
          val partitionIds = getLogicalPartitionPerState(state)(id)
          partitionIds.map(Return(_))
        } catch {
          case NonFatal(ex) =>
            logger.error("getLogicalPartition failed", ex)
            Seq(Throw(ex))
        }
      case None =>
        val ex = new NoShardIdException(s"cannot get shardId from $metadata")
        logger.error("getLogicalPartition failed", ex)
        Seq(Throw(ex))
    }
  }

  // listen to the WeightedAddress changes, transform the changes to a stream of
  // partition id (includes errors) to [[ServiceFactory]].
  private[this] val partitionAddressChanges: Activity[
    (B, Map[Try[Int], ServiceFactory[Req, Rep]])
  ] = {
    // the most complex layer of the stack we build here is the load balancer, which is pretty cheap
    // we can't just fix up the maps in-place because it leads to race conditions, so we recreate
    // them from scratch instead.
    addressedEndpoints
      .join(observable).map {
        case (factory, state) =>
          partitionAddressChangesCounter.incr()
          (
            getPartitionFunctionPerState(state), {
              // the raw grouping from updatePartitionMap, but without the update-in-place
              val grouped = groupBy(
                factory,
                { factory: EndpointFactory[Req, Rep] => getShardIdFromFactory(state, factory) })
              grouped.map {
                case (key, endpoints) =>
                  // This is so the loadbalancer knows that partitioning is enabled so that it
                  // doesn't close endpoints when closing the balancers.
                  // these endpoints eventually close by endpointsAsClosable.close()
                  val reusableEndpoints = endpoints.map(NotClosableEndpointFactoryProxy(_))
                  val paramsWithLB = params +
                    LoadBalancerFactory.Endpoints(
                      Var
                        .value(Activity.Ok(
                          reusableEndpoints.asInstanceOf[Set[EndpointFactory[_, _]]])).changes) +
                    LoadBalancerFactory.Dest(
                      Var.value(Addr.Bound(reusableEndpoints.map(_.address))))
                  key -> underlying.make(paramsWithLB)
              }
            })
      }.mapState {
        case f @ Activity.Failed(ex) =>
          updateFailures.incr()
          logger.error("partitionAddressChanges failed", ex)
          f
        case other => other
      }.stabilize
  }

  // Transform the stream of [[ServiceFactory]] to ServiceFactory and filter out
  // the failed partition id
  private[this] val partitionNodesChange: Event[SnapPartitioner[Req, Rep, B]] = {
    val init = SnapPartitioner.uninitialized[Req, Rep, B]
    partitionAddressChanges.states
      .foldLeft(init) {
        case (old, Activity.Ok((partitionFn, partitions))) =>
          // We close the old balancers because we otherwise leak gauges. We have set up
          // plumbing via the ReusableEndpoints stack param so that the balancers do not
          // close the endpoints when partitioning is enabled.
          old.partitionMapping.values.foreach(_.close())

          // this could possibly be an empty update if getLogicalPartition returns all Throws
          SnapPartitioner(
            partitionFn,
            partitions.collect {
              case (Return(key), sf) => key -> sf
            })
        case (staleState, _) => staleState
      }.filter(_.partitionMapping.nonEmpty)
  }

  private[this] val nodeWatcher: Closable =
    partitionNodesChange.register(Witness { v: SnapPartitioner[Req, Rep, B] =>
      snapUpdates.incr()
      partitionServiceNodes.set(v)
    })

  private[this] val endpointWatcher: Closable =
    addressedEndpoints.stabilize.values.register(Witness(addressedFactories))

  private[this] def endpointsAsClosable(): Closable = {
    addressedFactories.get match {
      case Return(setOfEndpoints) => Closable.all(setOfEndpoints.toSeq: _*)
      case Throw(_) => Closable.nop
    }
  }

  /**
   * Returns a [[SnapPartitioner]] which describes how to partition requests.
   */
  def snapshotSharder(): SnapPartitioner[Req, Rep, B] = partitionServiceNodes.get

  /**
   * When we close the node manager, all underlying services are closed.
   */
  def close(deadline: Time): Future[Unit] = {
    partitionerMetrics.remove()
    // we want to ensure that nodeWatcher stops updating the partitionServiceNodes
    // before we start closing them
    Closable
      .sequence(
        nodeWatcher,
        endpointWatcher,
        Closable.all(partitionServiceNodes.get.partitionMapping.values.toSeq: _*),
        endpointsAsClosable()
      ).close(deadline)
  }
}
