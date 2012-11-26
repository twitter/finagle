package com.twitter.finagle

import java.net.SocketAddress

package object client {
  /** Transform a service factory stack. */
  type Transformer[Req, Rep] = ServiceFactory[Req, Rep] => ServiceFactory[Req, Rep]
}
