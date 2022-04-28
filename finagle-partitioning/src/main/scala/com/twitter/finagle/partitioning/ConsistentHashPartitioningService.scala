package com.twitter.finagle.partitioning

import com.twitter.finagle
import com.twitter.finagle.Stack.Params
import com.twitter.finagle.loadbalancer.LoadBalancerFactory
import com.twitter.finagle.partitioning.param.NumReps
import com.twitter.finagle.{param => _, _}
import com.twitter.hashing._
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
  private[finagle] class NoPartitioningKeys(message: String) extends Exception(message)

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
}

private[finagle] abstract class ConsistentHashPartitioningService[Req, Rep, Key](
  underlying: Stack[ServiceFactory[Req, Rep]],
  params: Stack.Params,
  keyHasher: KeyHasher = KeyHasher.KETAMA,
  numReps: Int = NumReps.Default)
    extends PartitioningService[Req, Rep] {

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

  protected def partitionRequest(
    request: Req
  ): Future[Map[Req, Seq[Future[Service[Req, Rep]]]]] = {
    getPartitionKeys(request) match {
      case Seq(key) =>
        Future.value(Map(request -> Seq(partitionServiceForKey(key))))
      case keys: Seq[Key] if keys.nonEmpty =>
        groupByPartition(keys) match {
          case keyMap if keyMap.size == 1 =>
            // all keys belong to the same partition
            Future.value(Map(request -> Seq(partitionServiceForKey(keys.head))))
          case keyMap =>
            Future.value(
              keyMap.map {
                case (ps, pKeys) =>
                  (createPartitionRequestForKeys(request, pKeys.toSeq), Seq(ps))
              }
            )
        }
      case _ => noPartitionInformationHandler(request)
    }
  }

  protected[this] def groupByPartition(
    keys: Iterable[Key]
  ): Map[Future[Service[Req, Rep]], Iterable[Key]] =
    keys.groupBy(partitionServiceForKey)

  protected[this] def partitionServiceForKey(key: Key): Future[Service[Req, Rep]] =
    nodeManager.getServiceForHash(hashForKey(key))

  private[this] def hashForKey(key: Key): Long =
    keyHasher.hashKey(getKeyBytes(key))
}
