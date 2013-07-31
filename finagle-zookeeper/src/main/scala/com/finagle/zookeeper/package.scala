package com.finagle

import zookeeper.protocol.SerializableRecord
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}

package object zookeeper {

  /**
   * Conenience renamings.
   * TODO: These shluld go in favor of a better approach.
   *
   * Every request is made up from a header and a body, both binary serializable for being
   * sent over the wire.
   */
  type ZookeeperRequest = (Option[SerializableRecord], Option[SerializableRecord])
  type ZookeeperResponse = (Option[SerializableRecord], Option[SerializableRecord])

  def requestToChannelBuffer(request:ZookeeperRequest): ChannelBuffer = request match {
    case (header, body) =>{

      /**
       * Ensure the minimum space necesary.
       */
      val outputBuffer = ChannelBuffers.dynamicBuffer(4)

      outputBuffer.writeInt(0)

      header match {
        case Some(headerData) => headerData.serialize(outputBuffer)
        case None =>
      }

      body match {
        case Some(bodyData) => bodyData.serialize(outputBuffer)
        case None =>
      }

      /**
       * Write back the length information
       */
      outputBuffer.setInt(0, outputBuffer.writerIndex)

      outputBuffer
    }

    case _ => throw new UnsupportedOperationException()
  }

}
