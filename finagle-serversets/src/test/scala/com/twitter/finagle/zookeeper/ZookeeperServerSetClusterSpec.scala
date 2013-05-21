package com.twitter.finagle.zookeeper


import com.twitter.common.zookeeper.ServerSetImpl
import com.twitter.conversions.time._
import com.twitter.finagle.{NoBrokersAvailableException, Codec, CodecFactory, Service}
import com.twitter.finagle.builder.{ClientBuilder, Server, ServerBuilder}
import com.twitter.util.{Await, Future}
import java.net.InetSocketAddress
import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.frame.{
  Delimiters, DelimiterBasedFrameDecoder}
import org.jboss.netty.handler.codec.string.{StringEncoder, StringDecoder}
import org.jboss.netty.util.CharsetUtil
import org.specs.SpecificationWithJUnit

object StringCodec extends CodecFactory[String, String] {
  def server = Function.const {
    val pipeline = Channels.pipeline()
    pipeline.addLast("line",
      new DelimiterBasedFrameDecoder(100, Delimiters.lineDelimiter: _*))
    pipeline.addLast("stringDecoder", new StringDecoder(CharsetUtil.UTF_8))
    pipeline.addLast("stringEncoder", new StringEncoder(CharsetUtil.UTF_8))
    Codec.ofPipeline(pipeline)
  }

  def client = Function.const {
    val pipeline = Channels.pipeline()
    pipeline.addLast("stringEncode", new StringEncoder(CharsetUtil.UTF_8))
    pipeline.addLast("stringDecode", new StringDecoder(CharsetUtil.UTF_8))
    pipeline
    Codec.ofPipeline(pipeline)
  }
}

class ZookeeperServerSetClusterSpec extends SpecificationWithJUnit {
  "ZookeeperServerSetCluster" should {
    val inst = new ZkInstance
    import inst._
    doBefore(start())
    doAfter(stop())

    def withServer(f: Server => Unit) {
      val sillyService = new Service[String, String] {
        def apply(request: String) = Future(request.reverse)
      }

      val server = ServerBuilder()
        .codec(StringCodec)
        .bindTo(new InetSocketAddress(0))
        .name("ZKTestServer")
        .build(sillyService)

      f(server)
    }

    def withClient(cluster: ZookeeperServerSetCluster)(f: Service[String, String] => Unit) {
      val client = ClientBuilder()
        .cluster(cluster)
        .codec(StringCodec)
        .hostConnectionLimit(1)
        .build()

      f(client)
    }

    "register the server with ZooKeeper" in {
      val serverSet = new ServerSetImpl(zookeeperClient, "/twitter/services/silly")
      val cluster = new ZookeeperServerSetCluster(serverSet)

      withServer { server =>
        cluster.join(server.localAddress)

        withClient(cluster) { client =>
          cluster.thread.join()
          Await.result(client("hello\n"), 1.seconds) mustEqual "olleh"
        }
      }
    }

    "Be able to block till server set is ready" in {
      val serverSet = new ServerSetImpl(zookeeperClient, "/twitter/services/silly")
      val cluster = new ZookeeperServerSetCluster(serverSet)

      withServer { server =>
        withClient(cluster) { client =>
          var response: Future[String] = Future.value("init")
          val clientThread = new Thread {
            override def run {
              response = try {
                client("hello\n")
              } catch {
                case _ : NoBrokersAvailableException =>
                  true mustBe(false)
                  Future.value("fail")
              }
            }
          }

          clientThread.start()
          clientThread.join()
          cluster.ready.isDefined must beFalse
          cluster.join(server.localAddress)
          cluster.thread.join()
          Await.result(response) mustEqual "olleh"
          cluster.ready.isDefined must beTrue
        }
      }
    }

    "Be able to use an additional endpoint" in {
      val serverSet = new ServerSetImpl(zookeeperClient, "/twitter/services/silly")
      val cluster = new ZookeeperServerSetCluster(serverSet, "other-endpoint")

      withServer { server =>
        cluster.join(new InetSocketAddress(8000),
                     Map("other-endpoint" -> server.localAddress.asInstanceOf[InetSocketAddress]))

        withClient(cluster) { client =>
          cluster.thread.join()
          Await.result(client("hello\n"), 1.seconds) mustEqual "olleh"
        }
      }
    }

    "Ignore a server which does not specify the additional endpoint" in {
      val serverSet = new ServerSetImpl(zookeeperClient, "/twitter/services/silly")
      val cluster = new ZookeeperServerSetCluster(serverSet, "this-endpoint")

      withServer { server =>
        cluster.join(server.localAddress)
        cluster.ready.isDefined must beFalse
      }
    }
  }
}
