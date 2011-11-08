package com.twitter.finagle

import java.net.SocketAddress

/**
 * For exceptions you can catch, see [[com.twitter.finagle.RequestException]] and [[com.twitter.finagle.ApiException]]. 
 */

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
