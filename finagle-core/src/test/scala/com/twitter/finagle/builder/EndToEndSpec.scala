package com.twitter.finagle.builder

import com.twitter.finagle.integration.DynamicCluster
import com.twitter.util.{CountDownLatch, Promise}
import java.net.{InetSocketAddress, SocketAddress}
import org.specs.Specification
import org.jboss.netty.handler.codec.string.{StringEncoder, StringDecoder}
import org.jboss.netty.channel.{Channels, ChannelPipelineFactory}
import org.jboss.netty.handler.codec.frame.{Delimiters, DelimiterBasedFrameDecoder}
import org.jboss.netty.util.CharsetUtil
import com.twitter.finagle.{SimpleFilter, Codec, CodecFactory, Service, ServiceFactory}

class EndToEndSpec extends Specification {
  val constRes = new Promise[String]
  val arrivalLatch = new CountDownLatch(1)
  val service = new Service[String, String] {
    def apply(request: String) = {
      arrivalLatch.countDown()
      constRes
    }
  }

  "Finagle client" should {
    val address = new InetSocketAddress(0)
    val server = ServerBuilder()
      .codec(StringCodec)
      .bindTo(address)
      .name("FinagleServer")
      .build(service)

    val cluster = new DynamicCluster[SocketAddress](Seq(server.localAddress))
    val client = ClientBuilder()
      .cluster(cluster)
      .codec(StringCodec)
      .hostConnectionLimit(1)
      .build()

    "handle pending request after a host is deleted from cluster" in {
      // create a pending request; delete the server from cluster; then verify the request can still finish
      val response = client("123")
      arrivalLatch.await()
      cluster.del(server.localAddress)
      response.isDefined must beFalse
      constRes.setValue("foo")
      response() must be_==("foo")
    }
  }
}


object StringCodec extends StringCodec

class StringCodec extends CodecFactory[String, String] {
  def server = Function.const {
    new Codec[String, String] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline = {
          val pipeline = Channels.pipeline()
          pipeline.addLast("frameDecoder", new DelimiterBasedFrameDecoder(100, Delimiters.lineDelimiter: _*))
          pipeline.addLast("stringDecoder", new StringDecoder(CharsetUtil.UTF_8))
          pipeline.addLast("stringEncoder", new StringEncoder(CharsetUtil.UTF_8))
          pipeline
        }
      }
    }
  }

  def client = Function.const {
    new Codec[String, String] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline = {
          val pipeline = Channels.pipeline()
          pipeline.addLast("stringEncode", new StringEncoder(CharsetUtil.UTF_8))
          pipeline.addLast("stringDecode", new StringDecoder(CharsetUtil.UTF_8))
          pipeline
        }
      }
      
      override def prepareConnFactory(factory: ServiceFactory[String, String]) =
        (new AddNewlineFilter) andThen factory
    }
  }

  class AddNewlineFilter extends SimpleFilter[String, String] {
    def apply(request: String, service: Service[String, String]) = service(request + "\n")
  }
}
