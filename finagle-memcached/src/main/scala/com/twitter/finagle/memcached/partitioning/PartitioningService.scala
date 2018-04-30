package com.twitter.finagle.memcached.partitioning

import com.twitter.finagle._
import com.twitter.util._

/**
 * PartitioningService is responsible for request-key(s) based partition selection. Specially
 * built clients (e.g. memcached), that require partitioning support, insert it after the
 * BindingFactory and before LoadBalancerFactory. The only necessary configuration is
 * [[com.twitter.finagle.loadbalancer.LoadBalancerFactory.Dest]], which represents a changing
 * collection of addresses that the LoadBalancerFactory needs to operate on. The PartitioningService
 * manages the association of request key(s) to shards. At request time it examines the key(s)
 * present in the request and determines the partition(s) it belongs to. It then modifies the
 * original [[com.twitter.finagle.loadbalancer.LoadBalancerFactory.Dest]] param and therefore
 * narrows it down to the matching partition(s). As a result the per partition LoadBalancerFactories
 * operate only on a specific partition.
 */
private[finagle] abstract class PartitioningService[Req, Rep] extends Service[Req, Rep] {

  /**
   * Returns the partition that the request belongs to. All keys in the request are assumed to
   * belong to the same partition. Therefore the implementation can pick one of the keys and find
   * the partition the key belongs to.
   */
  protected def getPartitionFor(partitionedRequest: Req): Future[Service[Req, Rep]]

  /**
   * Extracts the necessary information (e.g. keys) that is needed for the partitioning logic and
   * groups them by partition. The keys present in the request could belong to multiple partitions.
   * The implementations will examine the keys, group them by partition, clone the request into
   * per-partition requests and return the sequence of partitioned requests.
   *
   * @param request: Incoming batched request
   * @return Sequence of partitioned requests
   */
  protected def partitionRequest(request: Req): Seq[(Req, Future[Service[Req, Rep]])]

  /**
   * This method is used for the batched request case. When the keys belong to multiple partitions,
   * the request is forked and sent to multiple partitions in parallel. After the responses come
   * back, this method is used to combine the response into a single Response, so that the combined
   * result can be returned back to the caller. Typically the implementations will be combining the
   * response key-value maps into a merged key-value map. The PartitioningService groups the
   * successful and failed responses from various partitions into 'successes' and 'failures' so that
   * the caller can retry the failed batches again. The mergeResponses implementations should
   * construct the response object accordingly.
   *
   * @param successes: successful responses
   * @param failures: map of failed partitioned requests against the exception
   * @return merged response
   */
  protected def mergeResponses(successes: Seq[Rep], failures: Map[Req, Throwable]): Rep

  protected[this] def applyService(request: Req, service: Future[Service[Req, Rep]]): Future[Rep] = {
    service.transform {
      case Return(svc) => svc(request)
      case t @ Throw(_) => Future.const(t.cast[Rep])
    }
  }

  protected[this] val partitionRequestFn = (t: (Req, Future[Service[Req, Rep]])) => {
    applyService(t._1, t._2)
  }

  private[this] val mergeResponsesFn = (seq: Seq[Either[(Req, Throwable), Rep]]) => {
    val (failures, successes) = seq.partition(_.isLeft)
    mergeResponses(successes.map(_.right.get), failures.map(_.left.get).toMap)
  }

  /**
   * Determines whether the request's keys live on only one partition or on more than one partition.
   * In the former case we can skip `partitionRequest` and immediately call `applyService`. In the
   * latter case we need to call `partitionRequest`.
   *
   * @param request: incoming request
   * @return whether the keys live on the same partition
   */
  protected def isSinglePartition(request: Req): Boolean

  final def apply(request: Req): Future[Rep] = {
    // Note that the services will be constructed in the implementing classes. So the
    // implementations will be responsible for closing them too.
    if (isSinglePartition(request)) {
      // single partition request (all keys belong to the same partition)
      applyService(request, getPartitionFor(request))
    } else {
      Future.collect(
        partitionRequest(request).map { case (pReq, service) =>
          partitionRequestFn((pReq, service)).transform {
            case Return(response) =>
              Future.value(Right(response))
            case Throw(exc) =>
              Future.value(Left((pReq, exc)))
          }
        }
      ).map(mergeResponsesFn)
    }
  }
}
