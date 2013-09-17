/**
 * Copyright 2012-2013  Foursquare Labs, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitter.finagle.dual

import com.twitter.conversions.time._
import com.twitter.finagle.{CodecFactory, Http => FinagleHttp,  Service}
import com.twitter.finagle.builder.{ClientBuilder, Server, ServerBuilder}
import com.twitter.finagle.dual.thrift.Adder
import com.twitter.finagle.http.{Http, Request, Response, RichHttp, Status}
import com.twitter.finagle.thrift.{ThriftClientFramedCodec, ThriftClientRequest, ThriftServerFramedCodec}
import com.twitter.util.{Await, Future, Time}
import java.net.InetSocketAddress
import org.apache.thrift.protocol.TBinaryProtocol
import org.jboss.netty.handler.codec.http.HttpResponseStatus
import org.specs.SpecificationWithJUnit

class FakeThriftCodecFactory extends CodecFactory[Array[Byte], Array[Byte]] {
  override def client: FakeThriftCodecFactory#Client = throw new UnsupportedOperationException

  override def server: FakeThriftCodecFactory#Server = { config =>
    ThriftServerFramedCodec.get().apply(config)
  }
}

class DualProtocolCodecTest extends SpecificationWithJUnit {
  "DualProtocolCodecFactory" should {
    "respond to both Thrift and HTTP requests on the same port" in {
      // Make the dual services for testing.

      val processor = new Adder.FutureIface {
        def add(x: Int, y: Int): Future[Int] = Future.value(x + y)
      }

      val thriftService = new Adder.FinagledService(processor, new TBinaryProtocol.Factory())

      val httpService = new Service[Request, Response] {
        override def apply(request: Request): Future[Response] = {
          request.uri match {
            case "/test" => Future.value(Response(Status.Ok))
            case _ => Future.value(Response(Status.NotFound))
          }
        }
      }

      val dualService = new Service[AnyRef, AnyRef] {
        override def apply(request: AnyRef): Future[AnyRef] = {
          request match {
            case httpRequest: Request => httpService(httpRequest)
            case _ => thriftService(request.asInstanceOf[Array[Byte]])  // guaranteed to be Array[Byte] if not a Request
          }
        }

        override def close(deadline: Time): Future[Unit] =
          Future.join(Seq(httpService.close(deadline), thriftService.close(deadline)))
      }

      val httpCodecFactory = RichHttp[Request](Http())

      val thriftCodecFactory = new FakeThriftCodecFactory

      val dualCodecFactory =
        new DualProtocolCodecFactory[Request, Array[Byte], Array[Byte]](httpCodecFactory, thriftCodecFactory)
      val dualCodec = dualCodecFactory.server

      val dualServer: Server = ServerBuilder()
        .bindTo(new InetSocketAddress(0))
        .codec(dualCodec)
        .name("dualserver")
        .build(dualService)

      val localSocketAddress = dualServer.localAddress.asInstanceOf[InetSocketAddress]
      val localHostPort = "127.0.0.1:%d" format localSocketAddress.getPort

      // Now make the clients used to access the dual services.

      val thriftServiceClient: Service[ThriftClientRequest, Array[Byte]] = ClientBuilder()
        .hosts(localHostPort)
        .codec(ThriftClientFramedCodec())
        .hostConnectionLimit(1)
        .build()

      val thriftClient = new Adder.FinagledClient(thriftServiceClient, new TBinaryProtocol.Factory())

      val httpClient: Service[Request, Response] = ClientBuilder()
        .hosts(localHostPort)
        .codec(RichHttp[Request](Http()))
        .hostConnectionLimit(1)
        .build()

      try {
        def checkThrift(x: Int, y: Int) {
          val expectedAnswer = x + y
          val remoteAnswer = Await.result(thriftClient.add(x, y), 5.seconds)
          remoteAnswer must_== expectedAnswer
        }

        def checkHttp(path: String, expectedStatus: HttpResponseStatus) {
          val request = Request(path)
          val response = Await.result(httpClient(request), 5.seconds)
          response.status must_== expectedStatus
        }

        checkThrift(5, 2025)
        checkHttp("/test", Status.Ok)
        checkHttp("/xyzzy", Status.NotFound)
        checkThrift(-204040, 8848484)
        checkHttp("/test", Status.Ok)
      } finally {
        Await.result(dualService.close(), 5.seconds)
      }
    }
  }

}
