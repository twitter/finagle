package com.twitter.finagle

package object client {
  /** Transform a service factory stack. */
  type Transformer[Req, Rep] = ServiceFactory[Req, Rep] => ServiceFactory[Req, Rep]
}
