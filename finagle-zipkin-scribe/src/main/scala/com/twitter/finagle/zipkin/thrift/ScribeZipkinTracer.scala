package com.twitter.finagle.zipkin.thrift

import com.twitter.finagle.zipkin.{core, host => Host}

/**
 * Receives the Finagle generated traces and sends a sample of them off to Zipkin via Scribe
 */
class ScribeZipkinTracer(tracer: ScribeRawZipkinTracer, sampler: core.Sampler)
    extends core.SamplingTracer(tracer, sampler) {

  /**
   * Default constructor for the service loader
   */
  private[finagle] def this() =
    this(
      ScribeRawZipkinTracer(
        scribeHost = Host().getHostName,
        scribePort = Host().getPort
      ),
      core.DefaultSampler
    )
}
