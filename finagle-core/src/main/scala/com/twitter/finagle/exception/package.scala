package com.twitter.finagle

import java.net.SocketAddress

package object exception {
  /**
   * A client is passed the service name
   */
  type ClientExceptionReceiverBuilder = String => ExceptionReceiver

  /**
   * A server factory is passed the service name and the SocketAddress that it is bound to
   */
  type ServerExceptionReceiverBuilder = (String, SocketAddress) => ExceptionReceiver
}
