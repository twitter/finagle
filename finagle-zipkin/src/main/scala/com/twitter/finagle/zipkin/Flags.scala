package com.twitter.finagle.zipkin

import com.twitter.app.GlobalFlag
import com.twitter.finagle.zipkin.thrift.Sampler
import java.net.InetSocketAddress

object Host extends GlobalFlag(
  new InetSocketAddress("localhost", 1463),
  "Host to scribe traces to")

object InitialSampleRate extends GlobalFlag(
  Sampler.DefaultSampleRate,
  "Initial sample rate")
