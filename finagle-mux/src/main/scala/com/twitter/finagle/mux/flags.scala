package com.twitter.finagle.mux

import com.twitter.app.GlobalFlag

object gracefulShutdownEnabled
  extends GlobalFlag(
    true,
    "Graceful shutdown enabled. " +
      "Temporary measure to allow servers to deploy without hurting clients."
  )
