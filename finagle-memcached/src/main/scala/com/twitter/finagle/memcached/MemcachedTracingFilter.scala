package com.twitter.finagle.memcached

import com.twitter.finagle._
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.memcached.protocol.Command
import com.twitter.finagle.memcached.protocol.Response
import com.twitter.finagle.memcached.protocol.RetrievalCommand
import com.twitter.finagle.memcached.protocol.Values
import com.twitter.finagle.partitioning.zk.ZkMetadata
import com.twitter.finagle.tracing.Trace
import com.twitter.util.Future
import com.twitter.util.Return

private[finagle] object MemcachedTracingFilter {
  val ShardIdAnnotationKey = "clnt/memcached.shard_id"
  val HitBooleanAnnotationKey = "clnt/memcached.hit"
  val HitsAnnotationKey = "clnt/memcached.hits"
  val MissesAnnotationKey = "clnt/memcached.misses"

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
  import MemcachedTracingFilter._

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
              val singleCommand = command.keys.size == 1
              if (singleCommand) {
                val isHit = command.keys.size == vals.size
                trace.recordBinary(HitBooleanAnnotationKey, isHit)
              } else {
                trace.recordBinary(HitsAnnotationKey, vals.size)
                trace.recordBinary(MissesAnnotationKey, command.keys.size - vals.size)
              }
            case _ =>
          }
        case _ =>
      }
    }

    response
  }
}

private final class ShardIdTracingFilter(addr: Address) extends SimpleFilter[Command, Response] {
  import MemcachedTracingFilter._

  def apply(command: Command, service: Service[Command, Response]): Future[Response] = {
    val trace = Trace
    if (trace.isActivelyTracing) {
      addr match {
        case Address.Inet(_, metadata) =>
          ZkMetadata.fromAddrMetadata(metadata) match {
            case Some(zkMetadata) =>
              zkMetadata.shardId match {
                case Some(shard) =>
                  trace.recordBinary(ShardIdAnnotationKey, shard)
                case _ => // no-op if the shard wasn't populated
              }
            case _ => // no-op if there is no metadata
          }
        case _ => // no-op
      }
    }

    service(command)
  }
}
