package com.twitter.finagle.netty4

import io.netty.util.AttributeKey

package object ssl {

  private[netty4] val ServerNameKey = AttributeKey.valueOf[String]("SSL_SERVER_NAME")
  
}
