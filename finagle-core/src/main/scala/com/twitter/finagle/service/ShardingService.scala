package com.twitter.finagle.service

import com.twitter.util.{Future, Closable, Time}
import com.twitter.hashing._
import com.twitter.finagle.{
  Service, Status, NotShardableException, ShardNotAvailableException}

/**
 * ShardingService takes a `Distributor` where the handle is a service.
 * It uses the distributor to distribute requests *only* when the hash function returns Some[Long].
 * If None is returned, it throws `NotShardableException`.
 * If the underlying service for a particular shard is not available, `NotServableException` is thrown.
 * Example:
 *   val serviceFactory =
 *     KetamaShardingServiceBuilder()
 *       .nodes(services) // services is of type Seq[(String, Service[Req,Rep])]
 *       .withHash { req => Some(hashCodeOfSomething)}
 *       .buildFactory()
 *   val service = serviceFactory()
 *   service(req) // where req is a Req and may have ShardableRequest mixed in
 */

class ShardingService[Req, Rep](
  distributor: Distributor[Service[Req, Rep]],
  hash: Req => Option[Long]
) extends Service[Req, Rep] {

  def apply(request: Req): Future[Rep] = {
    hash(request) map { hash =>
        val shard = distributor.nodeForHash(hash)
        // TODO: a sharding service may consider fine-grained statuses.
        if (shard.status != Status.Closed)
          shard(request)
        else
          Future.exception(ShardingService.ShardNotAvailableException)
    } getOrElse(Future.exception(ShardingService.NotShardableException))
  }

  override def status: Status = Status.bestOf[Service[Req, Rep]](distributor.nodes, _.status)
  override def close(deadline: Time) =
    Closable.all(distributor.nodes:_*).close(deadline)
}

private[service] object ShardingService {
  val NotShardableException = new NotShardableException
  val ShardNotAvailableException = new ShardNotAvailableException
}

case class KetamaShardingServiceBuilder[Req, Rep](
  _nodes: Option[Seq[KetamaNode[Service[Req, Rep]]]] = None,
  _hash: Option[Req => Option[Long]] = None,
  _numReps: Int = 160
) {

  def nodesAndWeights(nodes: Seq[(String, Int, Service[Req, Rep])]) = {
    copy(_nodes = Some(nodes map Function.tupled { KetamaNode(_,_,_) }))
  }

  def nodes(services: Seq[(String, Service[Req, Rep])]) = {
    nodesAndWeights(services map Function.tupled { (_, 1, _) })
  }

  def numReps(numReps: Int) = {
    copy(_numReps = numReps)
  }

  def withHash(f: Req => Option[Long]) = {
    copy(_hash = Some(f))
  }

  def buildFactory() = {
    if (_nodes.isEmpty) {
      throw new Exception("Nodes unspecified for KetamaShardingServiceBuilder")
    }
    if (_hash.isEmpty) {
      throw new Exception("Key function unspecified for KetamaShardingServiceBuilder")
    }

    val distributor = new KetamaDistributor(_nodes.get, _numReps)

    new SingletonFactory(new ShardingService(distributor, _hash.get))
  }
}
