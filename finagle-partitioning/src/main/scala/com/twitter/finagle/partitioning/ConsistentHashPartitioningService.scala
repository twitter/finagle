package com.twitter.finagle.partitioning

import com.twitter.finagle
import com.twitter.finagle.Stack.Params
import com.twitter.finagle.loadbalancer.LoadBalancerFactory
import com.twitter.finagle.param.Logger
import com.twitter.finagle.{param => _, _}
import com.twitter.hashing._
import com.twitter.logging.Level
import com.twitter.util._

/**
 * ConsistentHashPartitioningService implements consistent hashing based partitioning across the
 * 'CacheNodeGroup'. The group is dynamic and the hash ring is rebuilt upon observed changes
 * to the group. Optionally, unhealthy nodes are removed from the hash ring when
 * 'param.EjectFailedHost' param is true.
 */
private[finagle] object ConsistentHashPartitioningService {

  /**
   * Request is missing partitioning keys needed to determine target partition(s).
   */
  private[finagle] class NoPartitioningKeys extends Exception

  private[finagle] val DefaultNumReps = 160

  trait Module[Req, Rep, Key] extends Stack.Module[ServiceFactory[Req, Rep]] {

    val parameters = Seq(
      implicitly[Stack.Param[LoadBalancerFactory.Dest]],
      implicitly[Stack.Param[finagle.param.Stats]]
    )

    def newConsistentHashPartitioningService(
      underlying: Stack[ServiceFactory[Req, Rep]],
      params: Params
    ): ConsistentHashPartitioningService[Req, Rep, Key]

    final override def make(
      params: Params,
      next: Stack[ServiceFactory[Req, Rep]]
    ): Stack[ServiceFactory[Req, Rep]] = {
      val service: Service[Req, Rep] = newConsistentHashPartitioningService(next, params)
      Stack.leaf(role, ServiceFactory.const(service))
    }
  }

  /**
   * This method checks the given keys and returns true if they're all for the same partition in the
   * hash ring.
   * @param keys the keys to check
   * @param partitionIdForKey a function that converts a Key to the partition id for that key in the
   *                          hash ring
   */
  private[partitioning] def allKeysForSinglePartition[Key](
    keys: Iterable[Key],
    partitionIdForKey: Key => Long
  ): Boolean = {
    val kiter = keys.iterator
    var seenId = 0L
    var first = true

    while (kiter.hasNext) {
      val pid = partitionIdForKey(kiter.next())
      if (first) {
        first = false
        seenId = pid
      } else if (seenId != pid) {
        return false
      }
    }

    true
  }
}

private[finagle] abstract class ConsistentHashPartitioningService[Req, Rep, Key](
  underlying: Stack[ServiceFactory[Req, Rep]],
  params: Stack.Params,
  keyHasher: KeyHasher = KeyHasher.KETAMA,
  numReps: Int = ConsistentHashPartitioningService.DefaultNumReps)
    extends PartitioningService[Req, Rep] {

  import ConsistentHashPartitioningService._

  private[this] val logger = params[Logger].log

  private[this] val nodeManager = new HashRingNodeManager(underlying, params, numReps)

  /**
   * Returns the bytes for the key. For example if Key is a String, the implementation will
   * return key.getBytes(Charsets.UTF_8)
   */
  protected def getKeyBytes(key: Key): Array[Byte]

  /**
   * The classes extending ConsistentHashPartitioningService are expected to provide their own logic for
   * finding the "keys" (used in consistent hashing) from the request.
   */
  protected def getPartitionKeys(request: Req): Iterable[Key]

  /**
   * Use the original request and clone it to create a new Request with given set of keys. Used
   * for creating per partition requests. All keys passed to this function should belong to the
   * same partition.
   */
  protected def createPartitionRequestForKeys(original: Req, keys: Seq[Key]): Req

  override def close(deadline: Time): Future[Unit] = {
    Future.join(Seq(nodeManager.close(deadline), super.close(deadline)))
  }

  final override protected def getPartitionFor(
    partitionedRequest: Req
  ): Future[Service[Req, Rep]] = {
    val keys = getPartitionKeys(partitionedRequest)
    if (keys.isEmpty) {
      if (logger.isLoggable(Level.DEBUG))
        logger.log(Level.DEBUG, s"NoPartitioningKeys in getPartitionFor: $partitionedRequest")
      Future.exception(new NoPartitioningKeys())
    } else {
      // All keys in the request are assumed to belong to the same partition, so use the
      // first key to find the associated partition.
      partitionServiceForKey(keys.head)
    }
  }

  final override protected def partitionRequest(
    request: Req
  ): Seq[(Req, Future[Service[Req, Rep]])] = {
    getPartitionKeys(request) match {
      case Seq(key) =>
        Seq((request, partitionServiceForKey(key)))
      case keys: Seq[Key] if keys.nonEmpty =>
        groupByPartition(keys) match {
          case keyMap if keyMap.size == 1 =>
            // all keys belong to the same partition
            Seq((request, partitionServiceForKey(keys.head)))
          case keyMap =>
            keyMap.map {
              case (ps, pKeys) =>
                (createPartitionRequestForKeys(request, pKeys.toSeq), ps)
            }.toSeq
        }
      case _ =>
        if (logger.isLoggable(Level.DEBUG))
          logger.log(Level.DEBUG, s"NoPartitioningKeys in partitionRequest: $request")
        throw new NoPartitioningKeys
    }
  }

  /**
   * Extracts the keys from `req` and checks if they map to more than one partition. This
   * method will short circuit if it detects multiple partitions for efficiency's sake.
   *
   * It's intended to be used in `isSinglePartition` after any type tests, with the idea
   * that avoiding the merge phase for single-partition responses is worth paying the cost
   * of extracting and hashing the keys up front as part of this check.
   */
  protected def allKeysForSinglePartition(req: Req): Boolean =
    ConsistentHashPartitioningService.allKeysForSinglePartition(
      getPartitionKeys(req),
      partitionIdForKey
    )

  private[this] def groupByPartition(
    keys: Iterable[Key]
  ): Map[Future[Service[Req, Rep]], Iterable[Key]] =
    keys.groupBy(partitionServiceForKey)

  private[this] def hashForKey(key: Key): Long =
    keyHasher.hashKey(getKeyBytes(key))

  private[this] def partitionIdForKey(key: Key): Long =
    nodeManager.getPartitionIdForHash(hashForKey(key))

  private[this] def partitionServiceForKey(key: Key): Future[Service[Req, Rep]] =
    nodeManager.getServiceForHash(hashForKey(key))
}
