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
   * group them by partition. The keys present in the request could belong to multiple partitions.
   * The implementations will examine the keys, group them by partition, clone the request into
   * per-partition requests and return the sequence of partitioned requests
   *
   * @param request: Incoming batched request
   * @return Sequence of partitioned requests
   */
  protected def partitionRequest(request: Req): Seq[Req]

  /**
   * This method is used for the batched request case. When the keys belong to multiple partitions,
   * the request is forked and sent to multiple partitions in parallel. After the responses come
   * back, this method is used to combine the response into a single Response, so that the combined
   * result can be returned back to the caller. Typically the implementations will be combining the
   * response key-value maps into a merged key-value map.
   *
   * @param responses: per partition responses
   * @return merged response
   */
  protected def mergeResponses(responses: Seq[Rep]): Rep

  final def apply(request: Req): Future[Rep] = {
    // Note that the services will be constructed in the implementing classes. So the
    // implementations will be responsible for closing them too.
    partitionRequest(request) match {
      case Seq(_) =>
        // single partition request (all keys belong to the same partition)
        getPartitionFor(request).flatMap(_(request))
      case servicesSeq =>
        // multiple partitions
        Future
          .collect(
            servicesSeq.map { partitionedRequest =>
              getPartitionFor(partitionedRequest).flatMap(_(partitionedRequest))
            }
          )
          .map(mergeResponses)
    }
  }
}
