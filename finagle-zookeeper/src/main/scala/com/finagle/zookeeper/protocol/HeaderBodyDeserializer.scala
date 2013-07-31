package com.finagle.zookeeper.protocol

/**
 * A Zookeeper client receives responses in FIFO order.
 *
 * For every pending request an instance of HeaderBodyDeserializer sits
 * waiting to deserialize the response. The instance is created and added to
 * the pending request queue when the request is received.
 */
case class HeaderBodyDeserializer(headerDeserializer: RecordDeserializer,
                                  bodyDeserializer: RecordDeserializer) {



}
