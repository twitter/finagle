package com.finagle

import zookeeper.protocol.SerializableRecord

package object zookeeper {

  type ZookeeperRequest = SerializableRecord
  type ZookeeperResponse = SerializableRecord

}
