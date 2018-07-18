package com.twitter.finagle.redis.filter

import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.finagle.redis.protocol.{Command, Reply}
import com.twitter.finagle.redis.util.BufToString
import com.twitter.finagle.tracing.{Annotation, Trace}
import com.twitter.util.Future

private[redis] class RedisTracingFilter extends SimpleFilter[Command, Reply] {

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
