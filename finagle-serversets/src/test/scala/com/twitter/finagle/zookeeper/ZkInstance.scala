package com.twitter.finagle.zookeeper

import com.twitter.util.RandomSocket
import org.apache.zookeeper.server.{NIOServerCnxn, ZooKeeperServer}
import com.twitter.common.zookeeper.ZooKeeperClient
import org.apache.zookeeper.server.persistence.FileTxnSnapLog
import com.twitter.common.io.FileUtils.createTempDir
import com.twitter.common.quantity.{Amount, Time}

class ZkInstance {
  val zookeeperAddress = RandomSocket.nextAddress
  var connectionFactory: NIOServerCnxn.Factory = null
  var zookeeperServer: ZooKeeperServer = null
  var zookeeperClient: ZooKeeperClient = null

  def start() {
    zookeeperServer = new ZooKeeperServer(
      new FileTxnSnapLog(createTempDir(), createTempDir()),
      new ZooKeeperServer.BasicDataTreeBuilder)
    connectionFactory = new NIOServerCnxn.Factory(zookeeperAddress)
    connectionFactory.startup(zookeeperServer)
    zookeeperClient = new ZooKeeperClient(
      Amount.of(10, Time.MILLISECONDS),
      zookeeperAddress)

    // Disable noise from zookeeper logger
    java.util.logging.LogManager.getLogManager().reset();
  }

  def stop() {
    connectionFactory.shutdown()
    zookeeperClient.close()
  }
}