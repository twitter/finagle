package com.twitter.finagle.redis.filter

import com.twitter.finagle._
import com.twitter.finagle.redis.protocol.{Command, Reply}
import com.twitter.finagle.redis.util.BufToString
import com.twitter.finagle.tracing.{Annotation, Trace}
import com.twitter.util.Future

private[finagle] object RedisTracingFilter {
  val role: Stack.Role = Stack.Role("RedisTracing")
  val description: String = "redis client annotations"

  def module = new Stack.Module0[ServiceFactory[Command, Reply]] {
    val role: Stack.Role = RedisTracingFilter.role
    val description: String = RedisTracingFilter.description

    override def make(next: ServiceFactory[Command, Reply]): ServiceFactory[Command, Reply] = {
      new RedisTracingFilter().andThen(next)
    }
  }
}

private[finagle] class RedisTracingFilter extends SimpleFilter[Command, Reply] {

  def apply(command: Command, service: Service[Command, Reply]): Future[Reply] = {
    val trace = Trace()
    if (trace.isActivelyTracing) {
      trace.recordServiceName("redis")
      trace.recordRpc(BufToString(command.name))
      trace.record(Annotation.ClientSend)

      service(command).ensure(trace.record(Annotation.ClientRecv))
    } else service(command)
  }
}
