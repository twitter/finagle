package com.twitter.finagle.memcached.partitioning

import com.twitter.finagle.Stack.Params
import com.twitter.finagle._
import com.twitter.finagle.loadbalancer.LoadBalancerFactory
import com.twitter.finagle.param.Logger
import com.twitter.hashing._
import com.twitter.logging.Level
import com.twitter.util._
import scala.collection.breakOut

/**
 * KetamaPartitioningService implements consistent hashing based partitioning across the
 * 'CacheNodeGroup'. The group is dynamic and the hash ring is rebuilt upon observed changes
 * to the group. Optionally, unhealthy nodes are removed from the hash ring when
 * 'Memcached.param.EjectFailedHost' param is true.
 */
private[finagle] object KetamaPartitioningService {

  /**
   * Request is missing partitioning keys needed to determine target partition(s).
   */
  private[finagle] class NoPartitioningKeys extends Exception

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

  import KetamaPartitioningService._

  private[this] val logger = params[Logger].log

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
      if (logger.isLoggable(Level.DEBUG))
        logger.log(Level.DEBUG, s"NoPartitioningKeys in getPartitionFor: $partitionedRequest")
      Future.exception(new NoPartitioningKeys())
    } else {
      // All keys in the request are assumed to belong to the same partition, so use the
      // first key to find the associated partition.
      partitionForKey(keys.head)
    }
  }

  final override protected def partitionRequest(
    request: Req
  ): Seq[(Req, Future[Service[Req, Rep]])] = {
    getPartitionKeys(request) match {
      case Seq(key) =>
        Seq((request, partitionForKey(key)))
      case keys: Seq[Key] if keys.nonEmpty =>
        groupByPartition(keys) match {
          case keyMap if keyMap.size == 1 =>
            // all keys belong to the same partition
            Seq((request, partitionForKey(keys.head)))
          case keyMap =>
            keyMap.map {
              case (ps, pKeys) =>
                (createPartitionRequestForKeys(request, pKeys.toSeq), ps)
            }(breakOut)
        }
      case _ =>
        if (logger.isLoggable(Level.DEBUG))
          logger.log(Level.DEBUG, s"NoPartitioningKeys in partitionRequest: $request")
        throw new NoPartitioningKeys
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
    nodeManager.getServiceForHash(hash)
  }

}