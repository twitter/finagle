package com.twitter.finagle.memcached.partitioning

import com.twitter.finagle.Stack.Params
import com.twitter.finagle._
import com.twitter.finagle.loadbalancer.LoadBalancerFactory
import com.twitter.hashing._
import com.twitter.util._

/**
 * KetamaPartitioningService implements consistent hashing based partitioning across the
 * 'CacheNodeGroup'. The group is dynamic and the hash ring is rebuilt upon observed changes
 * to the group. Optionally, unhealthy nodes are removed from the hash ring when
 * 'Memcached.param.EjectFailedHost' param is true.
 */
private[finagle] object KetamaPartitioningService {

  private[finagle] val DefaultNumReps = 160

  trait Module[Req, Rep, Key] extends Stack.Module[ServiceFactory[Req, Rep]] {

    val parameters = Seq(
      implicitly[Stack.Param[LoadBalancerFactory.Dest]],
      implicitly[Stack.Param[param.Stats]]
    )

    def newKetamaPartitioningService(
      underlying: Stack[ServiceFactory[Req, Rep]],
      params: Params
    ): KetamaPartitioningService[Req, Rep, Key]

    final override def make(
      params: Params,
      next: Stack[ServiceFactory[Req, Rep]]
    ): Stack[ServiceFactory[Req, Rep]] = {
      val service: Service[Req, Rep] = newKetamaPartitioningService(next, params)
      Stack.Leaf(role, ServiceFactory.const(service))
    }
  }
}

private[finagle] abstract class KetamaPartitioningService[Req, Rep, Key](
  underlying: Stack[ServiceFactory[Req, Rep]],
  params: Stack.Params,
  keyHasher: KeyHasher = KeyHasher.KETAMA,
  numReps: Int = KetamaPartitioningService.DefaultNumReps
) extends PartitioningService[Req, Rep] {

  private[this] val nodeManager = new KetamaNodeManager(underlying, params, numReps)

  /**
   * Returns the bytes for the key. For example if Key is a String, the implementation will
   * return key.getBytes(Charsets.UTF_8)
   */
  protected def getKeyBytes(key: Key): Array[Byte]

  /**
   * The classes extending KetamaPartitioningService are expected to provide their own logic for
   * finding the "keys" (used in consistent hashing) from the request.
   */
  protected def getPartitionKeys(request: Req): Seq[Key]

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
      Future.exception(new IllegalStateException("No partition keys found in request"))
    } else {
      // All keys in the request are assumed to belong to the same partition, so use the
      // first key to find the associated partition.
      partitionForKey(keys.head)
    }
  }

  final override protected def partitionRequest(request: Req): Seq[Req] = {
    getPartitionKeys(request) match {
      case Seq(_) =>
        Seq(request)
      case keys =>
        groupByPartition(keys) match {
          case keyMap if keyMap.size == 1 =>
            // all keys belong to the same partition
            Seq(request)
          case keyMap =>
            keyMap.map {
              case (_, pKeys) =>
                createPartitionRequestForKeys(request, pKeys.toSeq)
            }.toSeq
        }
    }
  }

  private[this] def groupByPartition(
    keys: Iterable[Key]
  ): Map[Future[Service[Req, Rep]], Iterable[Key]] = {
    keys.groupBy(partitionForKey)
  }

  private[this] def partitionForKey(key: Key): Future[Service[Req, Rep]] = {
    val bytes = getKeyBytes(key)
    val hash = keyHasher.hashKey(bytes)
    nodeManager.getDistributor.nodeForHash(hash)
  }
}
