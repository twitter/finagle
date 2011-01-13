package com.twitter.finagle.zookeeper

import org.specs.Specification
import com.twitter.finagle.RandomSocket
import org.apache.zookeeper.server.{NIOServerCnxn, ZooKeeperServer}
import com.twitter.common.zookeeper.ZooKeeperClient
import com.twitter.common.quantity._
import com.twitter.common.io.FileUtils.createTempDir
import org.apache.zookeeper.server.persistence.FileTxnSnapLog
import com.twitter.finagle.thrift.{ThriftReply, SillyService}
import com.twitter.finagle.builder.{ClientBuilder, ServerBuilder, Thrift, ZookeeperPath}
import com.twitter.finagle.thrift.ThriftCall
import com.twitter.util.TimeConversions._
import com.twitter.silly.Silly
import org.apache.thrift.TBase

object ZookeeperServerSetClusterSpec extends Specification {
  "ZookeeperServerSetCluster" should {
    val zookeeperAddress = RandomSocket.nextAddress
    val zookeeperPath = ZookeeperPath("/twitter/services/silly")
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
      val sillyService = new SillyService()
      val server = ServerBuilder()
        .codec(Thrift)
        .zookeeperHosts(Seq(zookeeperAddress))
        .zookeeperPath(zookeeperPath)
        .service(sillyService)
        .bindTo(serviceAddress)
        .build()

      val client = ClientBuilder()
        .codec(Thrift)
        .zookeeperHosts(Seq(zookeeperAddress))
        .hosts(zookeeperPath)
        .buildService[ThriftCall.AnyCall, ThriftReply[_]]

      val call = new ThriftCall(
        "bleep",
        new Silly.bleep_args("hello"),
        classOf[Silly.bleep_result])

      client(call)().asInstanceOf[Silly.bleep_result].success mustEqual "olleh"
      1 mustEqual 1
    }
  }
}