package com.twitter.finagle.zipkin

import com.twitter.app.GlobalFlag
import com.twitter.finagle.zipkin.core.Sampler
import java.net.InetSocketAddress

object hosts extends GlobalFlag[Seq[InetSocketAddress]](
  Seq(new InetSocketAddress("localhost", 9092)),
  "Initial set of kafka servers to connect to, rest of cluster will be discovered (comma separated)")

object initialSampleRate extends GlobalFlag[Float](
  Sampler.DefaultSampleRate,
  "Initial sample rate")
