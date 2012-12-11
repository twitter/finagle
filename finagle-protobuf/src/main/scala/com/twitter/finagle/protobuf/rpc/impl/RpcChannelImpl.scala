package com.twitter.finagle.protobuf.rpc.impl

import java.net.InetSocketAddress
import com.google.protobuf.Descriptors.MethodDescriptor
import com.google.protobuf.RpcCallback
import com.google.protobuf.Message
import com.google.protobuf.RpcChannel
import com.google.protobuf.RpcController
import com.google.protobuf.Service
import org.slf4j.LoggerFactory
import com.twitter.util.Duration
import com.twitter.util.FuturePool
import com.twitter.finagle.builder.ClientBuilder
import java.util.concurrent.ExecutorService
import com.twitter.finagle.protobuf.rpc.RpcControllerWithOnFailureCallback
import com.twitter.finagle.protobuf.rpc.channel.ProtoBufCodec
import com.twitter.finagle.ChannelClosedException
import com.twitter.finagle.protobuf.rpc.Util
import com.twitter.finagle.protobuf.rpc.ExceptionResponseHandler

class RpcChannelImpl(cb: ClientBuilder[(String, Message), (String, Message), Any, Any, Any], s: Service, handler: ExceptionResponseHandler[Message], executorService: ExecutorService) extends RpcChannel {

  private val log = LoggerFactory.getLogger(getClass)

  private val futurePool = FuturePool(executorService)

  private val client: com.twitter.finagle.Service[(String, Message), (String, Message)] = cb
    .codec(new ProtoBufCodec(s))
    .unsafeBuild()

  def callMethod(m: MethodDescriptor, controller: RpcController,
    request: Message, responsePrototype: Message,
    done: RpcCallback[Message]): Unit = {
    // retries is a workaround for ChannelClosedException raised when servers shutdown.
    val retries = 3

    callMethod(m, controller, request, responsePrototype, done, retries)
  }

  def callMethod(m: MethodDescriptor, controller: RpcController,
    request: Message, responsePrototype: Message,
    done: RpcCallback[Message], retries: Int): Unit = {

    Util.log("Request", m.getName(), request)
    val req = (m.getName(), request)

    client(req) onSuccess { result =>
      Util.log("Response", m.getName(), result._2)
      futurePool({ handle(done, controller, result._2) })
    } onFailure { e =>
      log.warn("#callMethod# Failed.", e)
      e match {
        case cc: ChannelClosedException => if (retries > 1) {
          log.warn("#callMethod# Retrying.")
          callMethod(m, controller, request, responsePrototype, done, retries - 1);
        } else {
          controller.asInstanceOf[RpcControllerWithOnFailureCallback].setFailed(e)
        }
        case _ => controller.asInstanceOf[RpcControllerWithOnFailureCallback].setFailed(e)
      }
    }
  }

  def handle(done: RpcCallback[Message], controller: RpcController, m: Message) {
    if (handler != null && handler.canHandle(m)) {
      controller.asInstanceOf[RpcControllerWithOnFailureCallback].setFailed(handler.handle(m))
    } else {
      done.run(m)
    }
  }
  def release() {
    client.release()
  }

}
