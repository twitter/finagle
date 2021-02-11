package com.twitter.finagle.http.exp.routing

import com.twitter.finagle.http.Method

private[routing] final case class Schema(
  method: Method,
  path: Path)
