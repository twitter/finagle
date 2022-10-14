package com.twitter.finagle.partitioning

import com.twitter.finagle._
import com.twitter.finagle.tracing.Trace
import com.twitter.util._
import scala.collection.compat.immutable.ArraySeq

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

  import PartitioningService._

  def apply(request: Req): Future[Rep] = {
    makePartitionedRequests(request).map(doMergeResponses(request))
  }

  private[this] def makePartitionedRequests(req: Req): Future[Seq[(Req, Try[Rep])]] = {
    val trace = Trace()
    partitionRequest(req).flatMap { f =>
      Future.collect(f.flatMap {
        case (pReq, services) =>
          services.map {
            service =>
              // We go ahead and make peerIds without real concern for "skipping" an the original ID
              // because so long as the original is never populated, it won't appear in Zipkin or
              // break the relationship of data.
              Trace.letPeerId(trace.tracers) {
                applyService(pReq, service).transform { t =>
                  Future.value((pReq, t))
                }
              }
          }
      }.toSeq)
    }
  }

  private[this] def applyService(request: Req, fService: Future[Service[Req, Rep]]): Future[Rep] =
    fService.flatMap { svc => svc(request) }

  private[this] def doMergeResponses(request: Req)(result: Seq[(Req, Try[Rep])]): Rep = {
    result match {
      case Seq((_, tryResponse)) => tryResponse()
      case results: Seq[(Req, Try[Rep])] =>
        val success = ArraySeq.newBuilder[(Req, Rep)]
        val failure = ArraySeq.newBuilder[(Req, Throwable)]

        results.foreach {
          case (req, Return(rep)) => success += ((req, rep))
          case (req, Throw(t)) => failure += ((req, t))
        }

        mergeResponses(request, PartitionedResults(success.result(), failure.result()))
    }
  }

  /**
   * Extracts the necessary information (e.g. keys) that is needed for the partitioning logic and
   * groups them by partition. The keys present in the request could belong to multiple partitions.
   * The implementations will examine the keys, group them by partition, clone the request into
   * per-partition requests and return the map of partitioned requests.
   *
   * @param request: Incoming batched request
   * @return A map of the partitioned request to its service(s)
   *
   * @note Fanning out the same request to different partitions is supported in
   *       ThriftCustomPartitioning, while users can define their fan-out topology in
   *       CustomStrategy#getPartitionIdAndRequest. HashingStrategy always return a single service
   *       wrapped in Seq because hash keys are distributed to their unique partitions
   */
  protected def partitionRequest(request: Req): Future[Map[Req, Seq[Future[Service[Req, Rep]]]]]

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
   * @param originalReq: The non-partitioned request the user supplied
   * @param results: A PartitionedResults instance that encapsulates the successful and failed
   *                 responses of paired with their partitioned requests.
   * @return merged response
   */
  protected def mergeResponses(originalReq: Req, results: PartitionedResults[Req, Rep]): Rep

  /**
   * Error handling when processing requests to retrieve partition information failed, this is
   * implemented by each protocol to log proper information.
   */
  protected def noPartitionInformationHandler(req: Req): Future[Nothing]
}

object PartitioningService {

  /**
   * Encapsulates the success and failure pairs of the results of the partitioned requests.
   * The requests are paired with the responses so that they can be matched up with the keys
   * specified by the user and returned in that order.
   */
  protected[finagle] case class PartitionedResults[Req, Rep](
    successes: Seq[(Req, Rep)],
    failures: Seq[(Req, Throwable)])
}
