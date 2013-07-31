package com.finagle.zookeeper.protocol

import org.jboss.netty.buffer.ChannelBuffer

/**
 * A Zookeeper client receives responses in FIFO order.
 *
 * For every pending request an instance of HeaderBodyDeserializer sits
 * waiting to deserialize the response. The instance is created and added to
 * the pending request queue when the request is received.
 */
case class HeaderBodyDeserializer(headerDeserializer: RecordDeserializer = PassthroughDeserializer,
                                  bodyDeserializer: RecordDeserializer = PassthroughDeserializer) {

//  TODO: This should IndexOutOfBoundsException DRYed
  type ZookeeperResponse = (Option[SerializableRecord], Option[SerializableRecord])

  /**
   * A response is build by deserializing the header and the body.
   * @param inputBuffer
   * @return
   */
  def deserializeResponse(inputBuffer: ChannelBuffer): ZookeeperResponse = (
    headerDeserializer.deserialize(inputBuffer),
    bodyDeserializer.deserialize(inputBuffer)
    )

}

/**
 * When responses from Zookeeper lack a header or a body, a PassthroughDeserializer is used.
 * It doesn't alter in any way the ChannelBuffer it receives.
 */
object PassthroughDeserializer extends RecordDeserializer {

  def deserialize(input: MessageDeserializer) = None

}
