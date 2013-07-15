package com.twitter.finagle

package object server {
  type Transformer[Req, Rep] = ServiceFactory[Req, Rep] => ServiceFactory[Req, Rep]
}
