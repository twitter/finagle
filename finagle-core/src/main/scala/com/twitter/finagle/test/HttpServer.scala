package com.twitter.finagle.test

import java.net.InetSocketAddress

import org.jboss.netty.buffer._
import org.jboss.netty.handler.codec.http._

import com.twitter.finagle.builder._
import com.twitter.finagle._

import com.twitter.util.Future

object HttpServer {
  def main(args: Array[String]) {
    val server = new Service[HttpRequest, HttpResponse] {
      def apply(request: HttpRequest) = Future {
        val response = new DefaultHttpResponse(
          HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
        response.setContent(ChannelBuffers.wrappedBuffer("yo".getBytes))
        response
      }
    }

    ServerBuilder[HttpRequest, HttpResponse]()
      .codec(Http)
      .service(server)
      .bindTo(new InetSocketAddress(10000))
      .build
  }

  def quiesce() = ()
  def shutdown() = ()
}
