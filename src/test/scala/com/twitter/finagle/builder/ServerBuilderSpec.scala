package com.twitter.finagle.builder

import org.specs.Specification
import com.twitter.finagle.service.Service
import com.twitter.finagle.RandomSocket
import com.twitter.util.TimeConversions._
import org.jboss.netty.handler.codec.http._
import com.twitter.finagle.channel.ChannelClosedException
import com.twitter.util.{Time, Future}

object ServerBuilderSpec extends Specification {
  "ServerBuilder" should {
    "build a service with timeouts" in  {
      val aReallyLongTime = 4.seconds
      val mySlowAssService = new Service[HttpRequest, HttpResponse] {
        def apply(request: HttpRequest) = {
          Thread.sleep(aReallyLongTime.inMillis)
          Future(new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK))
        }
      }

      val addr = RandomSocket.nextAddress()

      val server = ServerBuilder()
        .codec(Http)
        .service(mySlowAssService)
        .requestTimeout(1.second)
        .bindTo(addr)
        .build()

      val client = ClientBuilder()
        .codec(Http)
        .hosts(Seq(addr))
        .buildService[HttpRequest, HttpResponse]

      val duration = Time.measure {
        client(new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "go/slow"))(aReallyLongTime) must throwA[ChannelClosedException]
      }

      duration mustEqual 1.second

      server.close().awaitUninterruptibly()
    }
  }
}