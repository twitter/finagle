package com.twitter.finagle.example.thrift

import java.net.InetSocketAddress

import org.apache.thrift.protocol.TBinaryProtocol

import com.twitter.finagle.{Service}
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.example.thriftscala.Hello
import com.twitter.finagle.thrift.{ThriftClientFramedCodec, ThriftClientRequest}
import com.twitter.util.Future


object ThriftClient {
  def main(args: Array[String]) {
    //#thriftclientapi
    val service: Service[ThriftClientRequest, Array[Byte]] = ClientBuilder()
      .hosts(new InetSocketAddress(8080))
      .build()

    val client = new Hello.FinagledClient(
      service, new TBinaryProtocol.Factory()
    )

    client.hi() onSuccess { response =>
      println("Received response: " + response)
      service.close()
    } onFailure { exp =>
      println("Received exception: " + exp)
      service.close()
    }
    //#thriftclientapi
  }
}
