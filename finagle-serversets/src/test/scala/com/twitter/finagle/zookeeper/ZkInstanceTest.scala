package com.twitter.finagle.zookeeper

import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.ZooDefs.Ids
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class ZkInstanceTest extends AnyFunSuite with BeforeAndAfter {
  @volatile var inst: ZkInstance = _

  before {
    inst = new ZkInstance
    inst.start()
  }

  after {
    inst.stop()
  }

  test("Basic validation test for in process zookeeper client and server") {
    val zkClient = inst.zookeeperClient.get
    val root = "/test"
    assert(root === zkClient.create(root, "".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT))
  }
}
