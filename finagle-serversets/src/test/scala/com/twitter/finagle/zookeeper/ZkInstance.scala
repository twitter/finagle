package com.twitter.finagle.zookeeper

import com.twitter.conversions.DurationOps._
import com.twitter.io.TempDirectory.create
import com.twitter.finagle.common.zookeeper.ZooKeeperClient
import com.twitter.zk.ServerCnxnFactory
import java.net.{InetAddress, InetSocketAddress}
import org.apache.zookeeper.server.ZooKeeperServer

class ZkInstance {
  var connectionFactory: ServerCnxnFactory = null
  var zookeeperServer: ZooKeeperServer = null
  var zookeeperClient: ZooKeeperClient = null
  var started = false

  lazy val zookeeperAddress = {
    if (!started) throw new IllegalStateException("can't get address until instance is started")
    new InetSocketAddress(InetAddress.getLoopbackAddress.getHostName, zookeeperServer.getClientPort)
  }

  lazy val zookeeperConnectString =
    zookeeperAddress.getHostName() + ":" + zookeeperAddress.getPort()

  def start(): Unit = {
    started = true

    zookeeperServer = new ZooKeeperServer(create(), create(), ZooKeeperServer.DEFAULT_TICK_TIME)
    zookeeperServer.setMaxSessionTimeout(100)
    zookeeperServer.setMinSessionTimeout(100)
    connectionFactory = ServerCnxnFactory(InetAddress.getLoopbackAddress)
    connectionFactory.startup(zookeeperServer)
    zookeeperClient = new ZooKeeperClient(10.milliseconds, zookeeperAddress)
  }

  def stop(): Unit = {
    connectionFactory.shutdown()
    zookeeperClient.close()
  }
}
