package com.twitter.finagle.zookeeper

import java.net.{InetAddress, InetSocketAddress}

import org.apache.zookeeper.server.{ZKDatabase, NIOServerCnxn, ZooKeeperServer}
import com.twitter.common.zookeeper.ZooKeeperClient
import org.apache.zookeeper.server.persistence.FileTxnSnapLog
import com.twitter.common.io.FileUtils.createTempDir
import com.twitter.common.quantity.{Amount, Time}

class ZkInstance {
  var connectionFactory: NIOServerCnxn.Factory = null
  var zookeeperServer: ZooKeeperServer = null
  var zookeeperClient: ZooKeeperClient = null
  var started = false

  lazy val zookeeperAddress = {
    if (!started) throw new IllegalStateException("can't get address until instance is started")
    new InetSocketAddress(zookeeperServer.getClientPort)
  }

  lazy val zookeeperConnectString =
    zookeeperAddress.getHostName() + ":" + zookeeperAddress.getPort();

  def start() {
    started = true

    val txn = new FileTxnSnapLog(createTempDir(), createTempDir())
    val zkdb = new ZKDatabase(txn)
    zookeeperServer = new ZooKeeperServer(
      txn,
      ZooKeeperServer.DEFAULT_TICK_TIME,
      100, 100,  // min/max sesssion timeouts in milliseconds
      new ZooKeeperServer.BasicDataTreeBuilder,
      zkdb
      )
    connectionFactory = new NIOServerCnxn.Factory(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))
    connectionFactory.startup(zookeeperServer)
    zookeeperClient = new ZooKeeperClient(
      Amount.of(10, Time.MILLISECONDS),
      zookeeperAddress)

    // Disable noise from zookeeper logger
//    java.util.logging.LogManager.getLogManager().reset();
  }

  def stop() {
    connectionFactory.shutdown()
    zookeeperClient.close()
  }
}
