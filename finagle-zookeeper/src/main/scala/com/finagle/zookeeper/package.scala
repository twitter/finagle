package com.finagle

import zookeeper.protocol.SerializableRecord

package object zookeeper {

  /**
   * Conenience renamings.
   * TODO: These shluld go in favor of a better approach.
   */
  type ZookeeperRequest = SerializableRecord
  type ZookeeperResponse = SerializableRecord

}
