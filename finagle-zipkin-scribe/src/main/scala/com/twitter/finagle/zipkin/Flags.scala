package com.twitter.finagle.zipkin

import com.twitter.app.GlobalFlag
import java.net.InetSocketAddress

object host
    extends GlobalFlag[InetSocketAddress](
      new InetSocketAddress("localhost", 1463),
      "Host to scribe traces to"
    )
