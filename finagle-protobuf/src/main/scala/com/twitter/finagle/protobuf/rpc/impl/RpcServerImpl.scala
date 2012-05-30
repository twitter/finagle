package com.twitter.finagle.protobuf.rpc.impl

import com.twitter.finagle.protobuf.rpc.channel.ProtoBufCodec
import com.twitter.finagle.protobuf.rpc.RpcServer
import com.twitter.util.{Future, Promise}
import com.twitter.util.Duration
import com.twitter.util.FuturePool
import com.twitter.finagle.builder.{ Server, ServerBuilder, ServerConfig }

import java.net.InetSocketAddress
import org.slf4j.LoggerFactory
import scala.None
import java.util.concurrent.Executors
import java.util.concurrent.ExecutorService

import com.google.common.base.Preconditions

import com.google.protobuf._
import com.google.protobuf.Descriptors.MethodDescriptor

class RpcServerImpl(sb: ServerBuilder[(String, Message), (String, Message), Any, Any, Any], port: Int, service: Service, executorService: ExecutorService) extends RpcServer {

  private val log = LoggerFactory.getLogger(getClass)

  Preconditions.checkNotNull(executorService)

  private val futurePool = FuturePool(executorService)

  private val server: Server = ServerBuilder.safeBuild(ServiceDispatcher(service, futurePool),
    sb
      .codec(new ProtoBufCodec(service))
      .name(getClass().getName())
      .bindTo(new InetSocketAddress(port)))

  def close(d: Duration) = {
    server.close(d)
  }
}

class ServiceDispatcher(service: com.google.protobuf.Service, futurePool: FuturePool) extends com.twitter.finagle.Service[(String, Message), (String, Message)] {

  private val log = LoggerFactory.getLogger(getClass)

  def apply(request: (String, Message)) = {

    val methodName = request._1
    val reqMessage = request._2

    val m = service.getDescriptorForType().findMethodByName(methodName);
    if (m == null) {
      throw new java.lang.AssertionError("Should never happen, we already decoded " + methodName)
    }

    // dispatch to the service method
    val task = {
      def promise = new Promise[(String, Message)]
      try {
        service.callMethod(m, null, reqMessage, new RpcCallback[Message]() {

          def run(msg: Message) = {
            promise.setValue((methodName, msg))
          }

        })
      } catch {
        case e: RuntimeException => {
          log.warn("#apply# Exception: ", e)
          promise.setValue((methodName, constructEmptyResponseMessage(m)))
        }
      }
      promise
    }
    Future.flatten(futurePool(task))
  }

  def constructEmptyResponseMessage(m: MethodDescriptor): Message = {
    m.getOutputType().toProto().getDefaultInstanceForType()
  }
}

object ServiceDispatcher {
  def apply(service: com.google.protobuf.Service, futurePool: FuturePool): ServiceDispatcher = { new ServiceDispatcher(service, futurePool) }
}




