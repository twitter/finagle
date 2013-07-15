package com.twitter.finagle.zipkin

import com.twitter.app.GlobalFlag
import com.twitter.finagle.zipkin.thrift.Sampler
import java.net.InetSocketAddress

object host extends GlobalFlag(
  new InetSocketAddress("localhost", 1463),
  "Host to scribe traces to")

object initialSampleRate extends GlobalFlag(
  Sampler.DefaultSampleRate,
  "Initial sample rate")
