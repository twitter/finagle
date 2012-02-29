package com.twitter.finagle.protobuf.rpc

import com.google.protobuf.Message
import com.google.protobuf.RpcChannel
import com.google.protobuf.Service
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.builder.ServerBuilder
import com.google.protobuf.RpcController
import com.twitter.util.Duration
import java.util.concurrent.ExecutorService

trait RpcFactory {

  def createServer(sb: ServerBuilder[(String, Message), (String, Message), Any, Any, Any], port: Int, service: Service, executorService: ExecutorService): RpcServer

  def createStub[T <: Service](cb: ClientBuilder[(String, Message), (String, Message), Any, Any, Any], service: { def newStub(c: RpcChannel): T }, executorService: ExecutorService): T

  def createController(): RpcController
}
