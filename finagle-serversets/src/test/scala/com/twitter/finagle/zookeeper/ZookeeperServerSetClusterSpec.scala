package com.twitter.finagle.zookeeper

import org.specs.Specification
import org.apache.zookeeper.server.{NIOServerCnxn, ZooKeeperServer}
import com.twitter.common.quantity._
import com.twitter.common.io.FileUtils.createTempDir
import org.apache.zookeeper.server.persistence.FileTxnSnapLog
import com.twitter.common.zookeeper.{ServerSetImpl, ZooKeeperClient}
import com.twitter.finagle.builder.{ClientBuilder, ServerBuilder}
import org.jboss.netty.handler.codec.string.{StringEncoder, StringDecoder}
import org.jboss.netty.util.CharsetUtil
import org.jboss.netty.handler.codec.frame.{Delimiters, DelimiterBasedFrameDecoder}
import com.twitter.util.{Future, RandomSocket}
import com.twitter.conversions.time._
import org.jboss.netty.channel._
import com.twitter.finagle.{Codec, CodecFactory, Service}

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

object ZookeeperServerSetClusterSpec extends Specification {
  "ZookeeperServerSetCluster" should {
    val zookeeperAddress = RandomSocket.nextAddress
    val serviceAddress = RandomSocket.nextAddress
    var connectionFactory: NIOServerCnxn.Factory = null
    var zookeeperServer: ZooKeeperServer = null
    var zookeeperClient: ZooKeeperClient = null

    doBefore {
      zookeeperServer = new ZooKeeperServer(
        new FileTxnSnapLog(createTempDir(), createTempDir()),
        new ZooKeeperServer.BasicDataTreeBuilder)
      connectionFactory = new NIOServerCnxn.Factory(zookeeperAddress)
      connectionFactory.startup(zookeeperServer)
      zookeeperClient = new ZooKeeperClient(
        Amount.of(100, Time.MILLISECONDS),
        zookeeperAddress)
    }

    doAfter {
      connectionFactory.shutdown()
      zookeeperClient.close()
    }

    "register the server with ZooKeeper" in {
      val serverSet = new ServerSetImpl(zookeeperClient, "/twitter/services/silly")
      val cluster = new ZookeeperServerSetCluster(serverSet)

      val sillyService = new Service[String, String] {
        def apply(request: String) = Future(request.reverse)
      }
      val server = ServerBuilder()
        .codec(StringCodec)
        .bindTo(serviceAddress)
        .name("ZKTestServer")
        .build(sillyService)

      cluster.join(serviceAddress)

      val client = ClientBuilder()
        .cluster(cluster)
        .codec(StringCodec)
        .hostConnectionLimit(1)
        .build()

      cluster.thread.join()
      client("hello\n")(1.seconds) mustEqual "olleh"
    }
  }
}
