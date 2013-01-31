package com.twitter.finagle.protobuf.rpc.impl

import com.twitter.finagle.protobuf.rpc.RpcFactory
import com.twitter.finagle.protobuf.rpc.RpcControllerWithOnFailureCallback
import com.twitter.finagle.protobuf.rpc.RpcServer


import java.util.concurrent.ExecutorService

class RpcFactoryImpl extends RpcFactory {

  def createServer(sb: ServerBuilder[(String, Message), (String, Message), Any, Any, Any], port: Int, service: Service, executorService: ExecutorService): RpcServer = new RpcServerImpl(sb, port, service, executorService)

  def createStub[T <: Service](cb: ClientBuilder[(String, Message), (String, Message), Any, Any, Any], service: { def newStub(c: RpcChannel): T }, executorService: ExecutorService): T = {
    service.newStub(new RpcChannelImpl(cb, service.asInstanceOf[T], executorService))
  }

  def createController(): RpcController = { new RpcControllerWithOnFailureCallback() }

  def release(stub: { def getChannel(): RpcChannel }) {
	stub.getChannel().asInstanceOf[RpcChannelImpl].close()
  }
}
