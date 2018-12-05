package com.twitter.finagle.thrift

import com.twitter.finagle.Service
import com.twitter.finagle.dispatch.SerialServerDispatcher
import com.twitter.finagle.transport.Transport
import com.twitter.util.Future

/**
 * Serial Dispatcher that avoids writing an empty array of bytes to the channel
 * which is indicative of a response to a 'oneway' message.
 */
private[finagle] class ThriftSerialServerDispatcher(
  transport: Transport[Array[Byte], Array[Byte]],
  service: Service[Array[Byte], Array[Byte]])
    extends SerialServerDispatcher[Array[Byte], Array[Byte]](transport, service) {

  override protected def handle(rep: Array[Byte]): Future[Unit] = {
    // Don't actually need to write an empty array, that is wasteful
    if (rep.length == 0) Future.Done
    else super.handle(rep)
  }
}
