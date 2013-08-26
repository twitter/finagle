package com.twitter.finagle.protobuf.rpc.impl

import com.twitter.finagle.protobuf.rpc.channel.ProtoBufCodec
import com.twitter.finagle.protobuf.rpc.{RpcServer, Util}
import com.twitter.util._
import com.twitter.util.Duration
import com.twitter.util.FuturePool
import com.twitter.finagle.builder.{Server, ServerBuilder, ServerConfig}
import java.net.InetSocketAddress
import java.util.logging.Logger
import scala.None
import java.util.concurrent.Executors
import java.util.concurrent.ExecutorService
import com.google.common.base.Preconditions
import com.twitter.finagle.protobuf.rpc.ServiceExceptionHandler
import com.google.protobuf.DynamicMessage
import com.google.protobuf.DynamicMessage.Builder
import com.google.protobuf._
import com.google.protobuf.Descriptors._

class RpcServerImpl(sb: ServerBuilder[(String, Message), (String, Message), Any, Any, Any], port: Int, service: Service, handler: ServiceExceptionHandler[Message], executorService: ExecutorService) extends RpcServer {

  private val log = Logger.getLogger(getClass.toString)

  Preconditions.checkNotNull(executorService)
  Preconditions.checkNotNull(handler)

  private val execFuturePool = new ExecutorServiceFuturePool(executorService)

  private val server: Server = ServerBuilder.safeBuild(ServiceDispatcher(service, handler, execFuturePool),
    sb
      .codec(new ProtoBufCodec(service))
      .name(getClass().getName())
      .bindTo(new InetSocketAddress(port)))

  def close(d: Duration) = {
    server.close(d)
  }
}

class ServiceDispatcher(service: com.google.protobuf.Service, handler: ServiceExceptionHandler[Message], futurePool: FuturePool) extends com.twitter.finagle.Service[(String, Message), (String, Message)] {

  private val log = Logger.getLogger(getClass.toString)

  def apply(request: (String, Message)) = {

    val methodName = request._1
    val reqMessage = request._2

    Util.log("Request", methodName, reqMessage)
    val m = service.getDescriptorForType().findMethodByName(methodName);
    if (m == null) {
      throw new java.lang.AssertionError("Should never happen, we already decoded " + methodName)
    }

    // dispatch to the service method
    val task = () => {
      var result = (methodName, constructEmptyResponseMessage(m))
      try {
        service.callMethod(m, null, reqMessage, new RpcCallback[Message]() {

          def run(msg: Message) = {
            Util.log("Response", methodName, msg)
            result =  (methodName, msg)
          }

        })
      } catch {
        case e: RuntimeException => {
          log.warning("#apply# Exception: "+e.getMessage)
          if (handler.canHandle(e)) {
            result = (methodName, handler.handle(e, constructEmptyResponseMessage(m)))
          }
        }
      }
      result
    }
    futurePool(task())
  }

  def constructEmptyResponseMessage(m: MethodDescriptor): Message = {
    val outputType = m.getOutputType();
    DynamicMessage.newBuilder(outputType).build()
  }
}

object ServiceDispatcher {
  def apply(service: com.google.protobuf.Service, handler: ServiceExceptionHandler[Message], futurePool: FuturePool): ServiceDispatcher = {
    new ServiceDispatcher(service, handler, futurePool)
  }
}




