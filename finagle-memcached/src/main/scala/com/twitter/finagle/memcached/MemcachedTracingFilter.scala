package com.twitter.finagle.memcached

import com.twitter.finagle._
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.memcached.protocol.{Command, Response, RetrievalCommand, Values}
import com.twitter.finagle.partitioning.zk.ZkMetadata
import com.twitter.finagle.tracing.Trace
import com.twitter.util.{Future, Return}

private[finagle] object MemcachedTracingFilter {

  /**
   * Apply the [[MemcachedTracingFilter]] protocol specific annotations
   */
  def memcachedTracingModule: Stackable[ServiceFactory[Command, Response]] =
    new Stack.Module1[param.Tracer, ServiceFactory[Command, Response]] {
      val role: Stack.Role = Stack.Role("MemcachedTracing")
      val description: String = "Add Memcached client specific annotations"

      def make(
        _tracer: param.Tracer,
        next: ServiceFactory[Command, Response]
      ): ServiceFactory[Command, Response] = {
        if (_tracer.tracer.isNull) next
        else (new MemcachedTracingFilter).andThen(next)
      }
    }

  /**
   * Apply the [[ShardIdTracingFilter]] that will annotate the shard id of a request's endpoint.
   */
  def shardIdTracingModule: Stackable[ServiceFactory[Command, Response]] =
    new Stack.Module2[Transporter.EndpointAddr, param.Tracer, ServiceFactory[Command, Response]] {
      val role: Stack.Role = Stack.Role("MemcachedShardIdTracer")
      val description: String = "Annotate the endpoint's shard id"

      def make(
        _transporter: Transporter.EndpointAddr,
        _tracer: param.Tracer,
        next: ServiceFactory[Command, Response]
      ): ServiceFactory[Command, Response] = {
        if (_tracer.tracer.isNull) next
        else new ShardIdTracingFilter(_transporter.addr).andThen(next)
      }
    }
}

private final class MemcachedTracingFilter extends SimpleFilter[Command, Response] {
  val hitsKey = "clnt/memcached.hits"
  val missesKey = "clnt/memcached.misses"

  def apply(command: Command, service: Service[Command, Response]): Future[Response] = {
    val trace = Trace()
    val response = service(command)
    if (trace.isActivelyTracing) {
      trace.recordRpc(command.name)

      // record hits/misses
      command match {
        case command: RetrievalCommand =>
          response.respond {
            case Return(Values(vals)) =>
              trace.recordBinary(hitsKey, vals.size)
              trace.recordBinary(missesKey, command.keys.size - vals.size)
            case _ =>
          }
        case _ =>
      }
    }

    response
  }
}

private final class ShardIdTracingFilter(addr: Address) extends SimpleFilter[Command, Response] {
  val shardIdKey = "clnt/memcached.shard_id"

  def apply(command: Command, service: Service[Command, Response]): Future[Response] = {
    val trace = Trace
    if (trace.isActivelyTracing) {
      addr match {
        case Address.Inet(_, metadata) =>
          val zkMetadata = ZkMetadata.fromAddrMetadata(metadata)

          // record the shard id if present in the metadata
          zkMetadata.foreach { zk =>
            zk.shardId.foreach(trace.recordBinary(shardIdKey, _))
          }

        case _ => // do nothing when there is no metadata
      }
    }

    service(command)
  }
}
